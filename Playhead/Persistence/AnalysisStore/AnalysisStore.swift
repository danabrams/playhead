// AnalysisStore.swift
// SQLite/FTS5 persistence for analysis pipeline state: transcription chunks,
// feature windows, ad windows, podcast profiles, and preview budgets.
// Separated from SwiftData because this data is append-heavy, versioned,
// needs FTS5, and supports resumable processing with checkpointing.

import CryptoKit
import Foundation
import OSLog
import SQLite3

/// SQLite SQLITE_TRANSIENT destructor constant — tells sqlite3_bind_text to
/// immediately copy the provided string. Defined once to avoid repeated
/// unsafeBitCast calls at every bind site.
private let SQLITE_TRANSIENT_PTR = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

// MARK: - Row types

/// playhead-h7r: the three artifact classes recognized by the storage
/// budget enforcer. Each `analysis_assets` row carries exactly one class;
/// per-class caps and eviction policies are encoded in ``StorageBudget``.
///
/// Raw-value strings are the ground-truth persisted in SQLite. Do not
/// rename them without a migration.
enum ArtifactClass: String, Sendable, Hashable, CaseIterable {
    /// Downloaded podcast media (audio file). Largest per-asset footprint;
    /// the only class that is directly evictable under dual-cap pressure.
    case media

    /// A warm-resume bundle (bucketed features / indices / compact
    /// derivatives) whose role is to accelerate resumption of analysis
    /// after a cold start. Never directly evicted; its footprint is
    /// constrained by a ratio check against evicted media (see
    /// ``StorageBudget``).
    case warmResumeBundle

    /// Short-lived intermediate state (temp staging, working buffers).
    /// Evicted AFTER media under dual-cap pressure. Safe to delete at
    /// any time — any lost scratch bytes are reconstructible.
    case scratch

    /// Decodes a raw SQLite value, defaulting to ``media`` for any
    /// unrecognized or empty string. `'media'` is the safe default
    /// because it is the only evictable class, matching the migration
    /// sentinel behavior (`DEFAULT 'media'`).
    static func fromPersistedRaw(_ raw: String?) -> ArtifactClass {
        guard let raw, let cls = ArtifactClass(rawValue: raw) else {
            return .media
        }
        return cls
    }
}

struct AnalysisAsset: Sendable {
    let id: String
    let episodeId: String
    let assetFingerprint: String
    let weakFingerprint: String?
    let sourceURL: String
    let featureCoverageEndTime: Double?
    let fastTranscriptCoverageEndTime: Double?
    let confirmedAdCoverageEndTime: Double?
    let analysisState: String
    let analysisVersion: Int
    let capabilitySnapshot: String?
    /// playhead-h7r: classification for storage-budget accounting.
    /// Defaults to ``ArtifactClass/media`` so every existing call-site
    /// (two in `AnalysisCoordinator`, one in `AnalysisWorkScheduler`)
    /// compiles unchanged — `media` is the correct class for the
    /// downloaded podcast audio they represent.
    let artifactClass: ArtifactClass
    /// playhead-gtt9.1.1: durable total-audio duration persisted at
    /// spool time. `nil` on legacy rows predating the column and on
    /// placeholder rows created before the pipeline has decoded audio
    /// yet (`resolveAssetId`). ``AnalysisCoordinator/resolveEpisodeDuration``
    /// treats `nil` or non-positive as "missing" and routes the
    /// coverage guards to their fail-safe shortcuts.
    ///
    /// Rationale: `activeShards` (the only other source of episode
    /// duration) is only populated during `spool` and is never
    /// rehydrated on resume-from-persisted-`.backfill` paths. Without
    /// this column the guards silently bypassed coverage checks on
    /// any process relaunch. See `playhead-gtt9.1` investigation.
    let episodeDurationSec: Double?
    /// playhead-gtt9.8: specific reason this asset landed in its
    /// current terminal state. Persisted so the harness can compute
    /// `scoredCoverageRatio` and distinguish "unscored because
    /// transcript never advanced" from "unscored because the
    /// classifier actually ran and disagreed" without reverse-
    /// engineering it from coverage watermarks.
    ///
    /// Written by `AnalysisCoordinator.finalizeBackfill` when the
    /// session transitions into one of the richer terminals
    /// (`completeFull`, `completeFeatureOnly`,
    /// `completeTranscriptPartial`, `failedTranscript`,
    /// `failedFeature`, `cancelledBudget`). `nil` on all legacy rows
    /// (pre-gtt9.8) and on sessions still in a non-terminal state.
    ///
    /// Example values: `"fullCoverage"`,
    /// `"transcriptionBudgetExceeded"`, `"transcriptFailed"`,
    /// `"featureFailed"`, `"budgetCancelled"`,
    /// `"coverageBelowThreshold"`.
    let terminalReason: String?
    /// playhead-i9dj: human-readable episode title (e.g. "How to escape
    /// burnout"). Defaults to `nil` so:
    ///   * existing call-sites construct an `AnalysisAsset` unchanged,
    ///   * pre-i9dj rows decode to `nil` for this field,
    ///   * the corpus exporter emits explicit JSON `null` when missing.
    /// Populated lazily on first observation via
    /// ``AnalysisStore/updateAssetEpisodeTitle(id:episodeTitle:)`` once
    /// the SwiftData side has the title.
    let episodeTitle: String?

    init(
        id: String,
        episodeId: String,
        assetFingerprint: String,
        weakFingerprint: String?,
        sourceURL: String,
        featureCoverageEndTime: Double?,
        fastTranscriptCoverageEndTime: Double?,
        confirmedAdCoverageEndTime: Double?,
        analysisState: String,
        analysisVersion: Int,
        capabilitySnapshot: String?,
        artifactClass: ArtifactClass = .media,
        episodeDurationSec: Double? = nil,
        terminalReason: String? = nil,
        episodeTitle: String? = nil
    ) {
        self.id = id
        self.episodeId = episodeId
        self.assetFingerprint = assetFingerprint
        self.weakFingerprint = weakFingerprint
        self.sourceURL = sourceURL
        self.featureCoverageEndTime = featureCoverageEndTime
        self.fastTranscriptCoverageEndTime = fastTranscriptCoverageEndTime
        self.confirmedAdCoverageEndTime = confirmedAdCoverageEndTime
        self.analysisState = analysisState
        self.analysisVersion = analysisVersion
        self.capabilitySnapshot = capabilitySnapshot
        self.artifactClass = artifactClass
        self.episodeDurationSec = episodeDurationSec
        self.terminalReason = terminalReason
        self.episodeTitle = episodeTitle
    }
}

struct AnalysisSession: Sendable {
    let id: String
    let analysisAssetId: String
    let state: String
    let startedAt: Double
    let updatedAt: Double
    let failureReason: String?
    /// bd-3bz (Phase 4): flag set when the Foundation Models shadow phase
    /// bailed on `canUseFoundationModels == false`. A capability observer in
    /// `PlayheadRuntime` drains sessions with this flag after FM recovers.
    /// Defaults to `false` so pre-existing rows decode identically.
    let needsShadowRetry: Bool
    /// bd-3bz (Phase 4): the podcastId captured at the point the shadow
    /// phase bailed. Needed to reconstruct the shadow-phase inputs during a
    /// retry drain without reaching back into the coordinator. `nil` unless
    /// `needsShadowRetry == true`.
    let shadowRetryPodcastId: String?

    init(
        id: String,
        analysisAssetId: String,
        state: String,
        startedAt: Double,
        updatedAt: Double,
        failureReason: String?,
        needsShadowRetry: Bool = false,
        shadowRetryPodcastId: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.state = state
        self.startedAt = startedAt
        self.updatedAt = updatedAt
        self.failureReason = failureReason
        self.needsShadowRetry = needsShadowRetry
        self.shadowRetryPodcastId = shadowRetryPodcastId
    }
}

struct FeatureWindow: Sendable {
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let rms: Double
    let spectralFlux: Double
    let musicProbability: Double
    let speakerChangeProxyScore: Double
    let musicBedChangeScore: Double
    let musicBedOnsetScore: Double
    let musicBedOffsetScore: Double
    let musicBedLevel: MusicBedLevel
    let pauseProbability: Double
    let speakerClusterId: Int?
    let jingleHash: String?
    let featureVersion: Int

    init(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        rms: Double,
        spectralFlux: Double,
        musicProbability: Double,
        speakerChangeProxyScore: Double = 0,
        musicBedChangeScore: Double = 0,
        musicBedOnsetScore: Double = 0,
        musicBedOffsetScore: Double = 0,
        musicBedLevel: MusicBedLevel = .none,
        pauseProbability: Double,
        speakerClusterId: Int?,
        jingleHash: String?,
        featureVersion: Int
    ) {
        self.analysisAssetId = analysisAssetId
        self.startTime = startTime
        self.endTime = endTime
        self.rms = rms
        self.spectralFlux = spectralFlux
        self.musicProbability = musicProbability
        self.speakerChangeProxyScore = speakerChangeProxyScore
        self.musicBedChangeScore = musicBedChangeScore
        self.musicBedOnsetScore = musicBedOnsetScore
        self.musicBedOffsetScore = musicBedOffsetScore
        self.musicBedLevel = musicBedLevel
        self.pauseProbability = pauseProbability
        self.speakerClusterId = speakerClusterId
        self.jingleHash = jingleHash
        self.featureVersion = featureVersion
    }
}

struct FeatureExtractionCheckpoint: Sendable, Equatable {
    let analysisAssetId: String
    let lastWindowStartTime: Double
    let lastWindowEndTime: Double
    let lastRms: Double
    let lastMusicProbability: Double
    let lastRawSpeakerChangeProxyScore: Double
    let penultimateRawSpeakerChangeProxyScore: Double?
    let lastMagnitudes: [Float]
    let featureVersion: Int
}

struct FeatureWindowSpeakerChangeProxyUpdate: Sendable, Equatable {
    let assetId: String
    let startTime: Double
    let endTime: Double
    let featureVersion: Int
    let speakerChangeProxyScore: Double
}

struct TranscriptChunk: Sendable {
    let id: String
    let analysisAssetId: String
    let segmentFingerprint: String
    let chunkIndex: Int
    let startTime: Double
    let endTime: Double
    let text: String
    let normalizedText: String
    let pass: String // fast | final
    let modelVersion: String
    let transcriptVersion: String?   // nil for fast-pass chunks (version computed on final)
    let atomOrdinal: Int?            // nil for fast-pass chunks
    let weakAnchorMetadata: TranscriptWeakAnchorMetadata?
    let speakerId: Int?              // B7: validated speaker label, nil when unavailable

    init(
        id: String,
        analysisAssetId: String,
        segmentFingerprint: String,
        chunkIndex: Int,
        startTime: Double,
        endTime: Double,
        text: String,
        normalizedText: String,
        pass: String,
        modelVersion: String,
        transcriptVersion: String?,
        atomOrdinal: Int?,
        weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil,
        speakerId: Int? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.segmentFingerprint = segmentFingerprint
        self.chunkIndex = chunkIndex
        self.startTime = startTime
        self.endTime = endTime
        self.text = text
        self.normalizedText = normalizedText
        self.pass = pass
        self.modelVersion = modelVersion
        self.transcriptVersion = transcriptVersion
        self.atomOrdinal = atomOrdinal
        self.weakAnchorMetadata = weakAnchorMetadata
        self.speakerId = speakerId
    }
}

struct AdWindow: Sendable {
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let confidence: Double
    let boundaryState: String
    let decisionState: String
    let detectorVersion: String
    let advertiser: String?
    let product: String?
    let adDescription: String?
    let evidenceText: String?
    let evidenceStartTime: Double?
    let metadataSource: String
    let metadataConfidence: Double?
    let metadataPromptVersion: String?
    let wasSkipped: Bool
    let userDismissedBanner: Bool
    let evidenceSources: String?
    let eligibilityGate: String?
    /// playhead-epfk: top `AdCatalogStore.matches(...)` similarity in
    /// `[0, 1]` for this window's acoustic fingerprint, computed at
    /// fusion time. `nil` when the catalog store was unavailable, the
    /// fingerprint was zero, or no match cleared the floor; `0.0` when
    /// the catalog ran but produced no positive match. Surfaced into
    /// `corpus-export.jsonl` so NARL eval can measure the
    /// fingerprint-store firing rate independently from the
    /// transcript-token catalog (which shares the `.catalog` evidence
    /// source label). Persisted on the row so a re-export of an old
    /// device DB doesn't lose the value.
    let catalogStoreMatchSimilarity: Double?

    init(
        id: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        confidence: Double,
        boundaryState: String,
        decisionState: String,
        detectorVersion: String,
        advertiser: String?,
        product: String?,
        adDescription: String?,
        evidenceText: String?,
        evidenceStartTime: Double?,
        metadataSource: String,
        metadataConfidence: Double?,
        metadataPromptVersion: String?,
        wasSkipped: Bool,
        userDismissedBanner: Bool,
        evidenceSources: String? = nil,
        eligibilityGate: String? = nil,
        catalogStoreMatchSimilarity: Double? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.startTime = startTime
        self.endTime = endTime
        self.confidence = confidence
        self.boundaryState = boundaryState
        self.decisionState = decisionState
        self.detectorVersion = detectorVersion
        self.advertiser = advertiser
        self.product = product
        self.adDescription = adDescription
        self.evidenceText = evidenceText
        self.evidenceStartTime = evidenceStartTime
        self.metadataSource = metadataSource
        self.metadataConfidence = metadataConfidence
        self.metadataPromptVersion = metadataPromptVersion
        self.wasSkipped = wasSkipped
        self.userDismissedBanner = userDismissedBanner
        self.evidenceSources = evidenceSources
        self.eligibilityGate = eligibilityGate
        self.catalogStoreMatchSimilarity = catalogStoreMatchSimilarity
    }
}

struct SkipCue: Sendable {
    let id: String
    let analysisAssetId: String
    let cueHash: String
    let startTime: Double
    let endTime: Double
    let confidence: Double
    let source: String      // "preAnalysis" | "live"
    let materializedAt: Double
    let wasSkipped: Bool
    let userDismissed: Bool
}

struct PodcastProfile: Sendable {
    let podcastId: String
    let sponsorLexicon: String?
    let normalizedAdSlotPriors: String?
    let repeatedCTAFragments: String?
    let jingleFingerprints: String?
    let implicitFalsePositiveCount: Int
    let skipTrustScore: Double
    let observationCount: Int
    let mode: String
    let recentFalseSkipSignals: Int
    /// ef2.5.1: JSON-encoded `ShowTraitProfile`. `nil` until first episode
    /// observation populates the profile. Decoded lazily by consumers.
    let traitProfileJSON: String?
    /// playhead-i9dj: human-readable show title (e.g. "Diary of a CEO").
    /// `nil` on pre-i9dj rows that have not been touched, and on the
    /// transient profile values that bounce through trust-scoring rebuilds
    /// without the title in scope. Defaults to `nil` so:
    ///   * existing PodcastProfile constructors compile unchanged,
    ///   * trust-scoring rebuilds that pass `title: nil` do NOT clobber
    ///     a previously-persisted title — `upsertProfile` uses
    ///     `COALESCE(excluded.title, podcast_profiles.title)` to preserve
    ///     the existing column value when the new write is `nil`.
    /// Populated via the dedicated
    /// ``AnalysisStore/updateProfileTitle(podcastId:title:)`` setter from
    /// the call site that owns the SwiftData `Podcast`.
    let title: String?

    init(
        podcastId: String,
        sponsorLexicon: String?,
        normalizedAdSlotPriors: String?,
        repeatedCTAFragments: String?,
        jingleFingerprints: String?,
        implicitFalsePositiveCount: Int,
        skipTrustScore: Double,
        observationCount: Int,
        mode: String,
        recentFalseSkipSignals: Int,
        traitProfileJSON: String? = nil,
        title: String? = nil
    ) {
        self.podcastId = podcastId
        self.sponsorLexicon = sponsorLexicon
        self.normalizedAdSlotPriors = normalizedAdSlotPriors
        self.repeatedCTAFragments = repeatedCTAFragments
        self.jingleFingerprints = jingleFingerprints
        self.implicitFalsePositiveCount = implicitFalsePositiveCount
        self.skipTrustScore = skipTrustScore
        self.observationCount = observationCount
        self.mode = mode
        self.recentFalseSkipSignals = recentFalseSkipSignals
        self.traitProfileJSON = traitProfileJSON
        self.title = title
    }

    /// Convenience: decode the stored trait profile, falling back to
    /// `ShowTraitProfile.unknown` when absent or corrupt.
    var traitProfile: ShowTraitProfile {
        guard let json = traitProfileJSON,
              let data = json.data(using: .utf8),
              let decoded = try? JSONDecoder().decode(ShowTraitProfile.self, from: data)
        else { return .unknown }
        return decoded
    }
}

/// bd-m8k: Per-podcast CoveragePlanner state. Sibling row to
/// `PodcastProfile`; persisted in the `podcast_planner_state` table that the
/// v4 migration creates. Rows are upserted lazily on first observation, never
/// backfilled. `recallSamples` is the most-recent-up-to-3 ring of full-
/// rescan **recall** measurements (oldest first); the cached
/// `stableRecallFlag` reflects the result of evaluating both the episode-
/// count floor and the recall threshold against this ring at the moment
/// of the last write.
///
/// Cycle 2 C4: the metric was historically misnamed "precision" — it is
/// actually recall (covered / actual ad line refs). The struct fields use
/// the corrected name; the persisted SQLite columns and JSON keys keep the
/// legacy `precision*` names so existing v4 rows decode without a
/// migration. Each storage boundary is annotated with
/// `// historical: stored as "precision"; semantically recall`.
struct PodcastPlannerState: Sendable, Equatable {
    let podcastId: String
    let observedEpisodeCount: Int
    let episodesSinceLastFullRescan: Int
    /// Cycle 2 C4: stored as `stablePrecisionFlag` in SQLite; semantically
    /// the stable-recall flag. See type-level doc.
    let stableRecallFlag: Bool
    let lastFullRescanAt: Double?
    /// Most recent up to `AnalysisStore.plannerRecallRingSize` full-rescan
    /// recall samples. Oldest first; new samples are appended and the
    /// oldest dropped on overflow.
    /// Cycle 2 C4: stored across `precisionSample1..3` columns; semantically
    /// recall. See type-level doc.
    let recallSamples: [Double]
    /// Cycle 4 B4: per-podcast running total of episodes observed that
    /// produced no recall sample (ad-free full rescans). Persisted on
    /// `podcast_planner_state` so the counter accrues across
    /// `BackfillJobRunner` instances and across process restarts — the
    /// runner-level counter of the same name is per-run only and was
    /// therefore always 0 or 1 when read. Legacy rows that predate this
    /// column decode as 0.
    let episodesObservedWithoutSampleCount: Int
    /// Cycle 4 B4: per-podcast running total of episodes where every
    /// non-fullEpisodeScan narrowing phase returned `wasEmpty == true`.
    /// Increments fire on BOTH full rescans and live targeted-with-audit
    /// runs, so the counter captures the cross-phase empty signal that
    /// individual `narrowing.empty.{phase}` cannot. Legacy rows decode
    /// as 0.
    let narrowingAllPhasesEmptyEpisodeCount: Int
}

struct PreviewBudget: Sendable {
    let canonicalEpisodeKey: String
    let consumedAnalysisSeconds: Double
    let graceBreakWindow: Double
    let lastUpdated: Double
}

/// playhead-uzdq: minimal return value from
/// ``AnalysisStore/acquireEpisodeLease``. The store side doesn't carry
/// preemption signals or in-memory checkpoint caches — those live on
/// the coordinator's richer ``EpisodeExecutionLease`` struct. This
/// descriptor just plumbs the persisted state back to the caller.
struct EpisodeExecutionLeaseDescriptor: Sendable, Equatable {
    let jobId: String
    let episodeId: String
    let ownerWorkerId: String
    let generationID: String
    let schedulerEpoch: Int
    let acquiredAt: Double
    let expiresAt: Double
}

struct AnalysisJob: Sendable {
    let jobId: String
    let jobType: String         // "preAnalysis" | "playback" | "backfill"
    let episodeId: String
    let podcastId: String?
    let analysisAssetId: String?
    let workKey: String         // fingerprint + analysisVersion + jobType
    let sourceFingerprint: String
    let downloadId: String
    let priority: Int
    let desiredCoverageSec: Double
    let featureCoverageSec: Double
    let transcriptCoverageSec: Double
    let cueCoverageSec: Double
    let state: String
    let attemptCount: Int
    let nextEligibleAt: Double?
    let leaseOwner: String?
    let leaseExpiresAt: Double?
    let lastErrorCode: String?
    let createdAt: Double
    let updatedAt: Double
    /// playhead-uzdq: generation ID owned by the current lease holder,
    /// or `""` when no lease has ever been acquired. Stored as
    /// `generationID TEXT NOT NULL DEFAULT ''` and read back as the
    /// empty string for pre-uzdq rows. Callers that need a typed
    /// identity use ``EpisodeExecutionLease/generationID`` (UUID).
    let generationID: String
    /// playhead-uzdq: scheduler epoch captured when the current lease
    /// holder acquired the row, or `0` when no lease has ever been
    /// acquired.
    let schedulerEpoch: Int
    /// playhead-bnrs: primary artifact class this job will write to.
    /// In-memory only (NOT persisted in `analysis_jobs`); consumed by
    /// `AdmissionGate` at admission time. Defaults to `.media` so every
    /// existing call-site compiles unchanged — media is the dominant
    /// write class for pre-analysis jobs.
    let artifactClass: ArtifactClass
    /// playhead-bnrs: caller-estimated write bytes for the next slice
    /// this job will emit. In-memory only (NOT persisted); consumed by
    /// `AdmissionGate` as a headroom input for per-class storage slice
    /// sizing. Defaults to `0` so existing call-sites compile unchanged;
    /// downstream beads that care about precise sizing can set this
    /// explicitly at enqueue time.
    let estimatedWriteBytes: Int64

    // Existing-style memberwise init that defaults the uzdq fields so
    // every call-site in the codebase compiles unchanged. New callers
    // pass the two uzdq fields explicitly. The playhead-bnrs fields
    // (`artifactClass`, `estimatedWriteBytes`) follow the same defaulted
    // pattern so the admission-gate integration adds no call-site churn.
    init(
        jobId: String,
        jobType: String,
        episodeId: String,
        podcastId: String?,
        analysisAssetId: String?,
        workKey: String,
        sourceFingerprint: String,
        downloadId: String,
        priority: Int,
        desiredCoverageSec: Double,
        featureCoverageSec: Double,
        transcriptCoverageSec: Double,
        cueCoverageSec: Double,
        state: String,
        attemptCount: Int,
        nextEligibleAt: Double?,
        leaseOwner: String?,
        leaseExpiresAt: Double?,
        lastErrorCode: String?,
        createdAt: Double,
        updatedAt: Double,
        generationID: String = "",
        schedulerEpoch: Int = 0,
        artifactClass: ArtifactClass = .media,
        estimatedWriteBytes: Int64 = 0
    ) {
        self.jobId = jobId
        self.jobType = jobType
        self.episodeId = episodeId
        self.podcastId = podcastId
        self.analysisAssetId = analysisAssetId
        self.workKey = workKey
        self.sourceFingerprint = sourceFingerprint
        self.downloadId = downloadId
        self.priority = priority
        self.desiredCoverageSec = desiredCoverageSec
        self.featureCoverageSec = featureCoverageSec
        self.transcriptCoverageSec = transcriptCoverageSec
        self.cueCoverageSec = cueCoverageSec
        self.state = state
        self.attemptCount = attemptCount
        self.nextEligibleAt = nextEligibleAt
        self.leaseOwner = leaseOwner
        self.leaseExpiresAt = leaseExpiresAt
        self.lastErrorCode = lastErrorCode
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.generationID = generationID
        self.schedulerEpoch = schedulerEpoch
        self.artifactClass = artifactClass
        self.estimatedWriteBytes = estimatedWriteBytes
    }

    static func computeWorkKey(fingerprint: String, analysisVersion: Int, jobType: String) -> String {
        "\(fingerprint):\(analysisVersion):\(jobType)"
    }
}

// MARK: - Store errors

enum AnalysisStoreError: Error, CustomStringConvertible, Equatable {
    case openFailed(code: Int32, message: String)
    case migrationFailed(String)
    case queryFailed(String)
    case insertFailed(String)
    case notFound
    case duplicateJobId(String)
    case invalidRow(column: Int)
    case invalidEvidenceEvent(String)
    case invalidScanCohortJSON(String)
    /// C-2: raised when a backfill-job state transition is attempted against
    /// a row whose current status does not permit it (e.g. transitioning a
    /// `.complete` or `.failed` row back into `.running`).
    ///
    /// Fix #6: `fromStatus` carries the row's prior status at the moment
    /// the transition was rejected. It is `nil` when no row existed for
    /// the `jobId`, so callers can distinguish "missing" from "in a
    /// specific terminal state" without re-querying.
    case invalidStateTransition(jobId: String, fromStatus: String?, toStatus: String)
    /// H-2: raised when `insertEvidenceEvent` encounters a PRIMARY KEY
    /// collision where the existing row's body (evidenceJSON/createdAt)
    /// differs from the incoming row. The M-4 INSERT OR IGNORE path was
    /// silently preserving the stored body; callers that truly collide on
    /// id but with different content now get a loud failure.
    case evidenceEventBodyMismatch(id: String)
    /// playhead-4my.10.1 (L5): a JSON encoder produced bytes we could not
    /// decode. Foundation's `JSONEncoder` should never produce non-UTF8
    /// output, so this is structural — we surface it instead of
    /// fall-through-defaulting the persisted column to `"[]"` (which
    /// would silently mask future regressions).
    case encodingFailure(String)

    var description: String {
        switch self {
        case .openFailed(let code, let msg): "SQLite open failed (\(code)): \(msg)"
        case .migrationFailed(let msg): "Migration failed: \(msg)"
        case .queryFailed(let msg): "Query failed: \(msg)"
        case .insertFailed(let msg): "Insert failed: \(msg)"
        case .notFound: "Row not found"
        case .duplicateJobId(let id): "Duplicate backfill job id: \(id)"
        case .invalidRow(let col): "Unexpected NULL in non-null column \(col)"
        case .invalidEvidenceEvent(let msg): "Invalid evidence event: \(msg)"
        case .invalidScanCohortJSON(let msg): "Invalid scanCohortJSON: \(msg)"
        case .invalidStateTransition(let id, let from, let to):
            "Invalid backfill job state transition for \(id): \(from ?? "<missing>") -> \(to)"
        case .evidenceEventBodyMismatch(let id):
            "Evidence event id '\(id)' already persisted with a different body"
        case .encodingFailure(let msg):
            "Encoding failure: \(msg)"
        }
    }
}

// MARK: - AnalysisStore actor

actor AnalysisStore {

    nonisolated private static let currentSchemaVersion = 17

    /// bd-m8k / Cycle 2 C4: Maximum number of recent full-rescan **recall**
    /// samples retained for the `stable_recall_flag` ring. Must match the
    /// column count in `podcast_planner_state` and the push/shift logic in
    /// `recordPodcastEpisodeObservation`. The persisted columns are still
    /// named `precisionSample{1,2,3}` / `precisionSampleCount`; the
    /// in-memory rename is code-only.
    nonisolated static let plannerRecallRingSize = 3

    /// bd-m8k / Cycle 2 C4: Minimum per-sample recall required for
    /// `stable_recall_flag` to flip true. All samples in the ring must
    /// clear this threshold. The persisted column is still named
    /// `stablePrecisionFlag`; semantically recall.
    nonisolated static let plannerRecallThreshold: Double = 0.85

    /// bd-m8k: Minimum `observed_episode_count` before
    /// `stable_precision_flag` is permitted to be true. Mirrors
    /// `CoveragePlanner.defaultColdStartEpisodeThreshold`.
    nonisolated static let plannerStableObservedEpisodeFloor = 5

    #if DEBUG
    enum FeatureBatchPersistenceFaultInjection: Equatable {
        case afterCoverageUpdateBeforeCommit
    }

    private var featureBatchPersistenceFaultInjection: FeatureBatchPersistenceFaultInjection?

    /// playhead-uhdu (5uvz.1 NIT #1): test-only fault injection points
    /// for `acquireLeaseWithJournal`. Lets tests force the inner journal
    /// append to throw so the rollback contract — phantom-lease impossible
    /// by construction — is validated by execution rather than visual
    /// inspection of the catch block.
    enum LeaseJournalFaultInjection: Equatable {
        /// Throw between the lease UPDATE and the journal append.
        /// Validates that the catch block rolls back the UPDATE so no
        /// phantom-lease (lease held, no journal trail) survives.
        case afterUpdateBeforeJournalAppend
    }

    private var leaseJournalFaultInjection: LeaseJournalFaultInjection?

    /// playhead-5uvz.3 (Gap-3): test-only fault-injection points for the
    /// `AnalysisWorkScheduler.processJob` outcome arms. The arms now run
    /// inside a single `runSchedulingPass` transaction so progress, the
    /// state-specific writes, and the lease release commit or roll back
    /// together. These checkpoints let tests force a throw between the
    /// individual writes inside that transaction and verify the row's
    /// pre-arm state is preserved (no half-finalized terminal mark).
    enum ProcessJobOutcomeFaultInjection: Equatable {
        /// Throw after the progress UPDATE but before the state UPDATE.
        /// Validates that the progress write rolls back so the row keeps
        /// its previous coverage and is requeued cleanly by orphan recovery.
        case afterProgressUpdateBeforeStateUpdate
        /// Throw after the state UPDATE but before the final lease release.
        /// Validates that the terminal-mark write rolls back so the row
        /// is not stranded with `state=complete` but a still-held lease.
        case afterStateUpdateBeforeLeaseRelease
    }

    private var processJobOutcomeFaultInjection: ProcessJobOutcomeFaultInjection?
    #endif

    /// bd-1tl: dedicated logger for store-level diagnostics that should
    /// reach Console.app on real devices without test scaffolding.
    private let logger = Logger(subsystem: "com.playhead", category: "AnalysisStore")

    /// The raw SQLite handle. Marked `nonisolated(unsafe)` so deinit can close
    /// it without requiring actor isolation (Swift 6 strict concurrency).
    /// All actual usage is funnelled through actor-isolated methods.
    nonisolated(unsafe) private var db: OpaquePointer?

    /// Path to the SQLite database file.
    nonisolated let databaseURL: URL

    /// playhead-6boz: when non-nil, the on-disk file at `databaseURL` is
    /// expected to live inside an Application Support container that may
    /// not exist yet on first launch. `ensureOpen()` performs the
    /// `createDirectory` + Data Protection setattr work the first time.
    /// `path:` initializers (e.g. `:memory:`, raw test paths) skip that
    /// dance entirely (this is `nil` for them).
    nonisolated private let containerDirectoryToCreate: URL?

    /// playhead-6boz: the raw string handed to `sqlite3_open_v2` from
    /// `ensureOpen()`. For the `directory:` initializer this is
    /// `databaseURL.path`. For the `path:` initializer it's the literal
    /// path string the caller supplied (preserving `:memory:`, which
    /// `URL(fileURLWithPath:)` would mangle into a cwd-rooted path).
    nonisolated private let sqliteOpenPath: String

    /// playhead-6boz: schema-readiness flag. Flips to `true` after the
    /// first successful `ensureOpen()` (open + pragmas + migration).
    /// Public read surface is `isOpen` (async, actor-isolated).
    private var didOpen: Bool = false

    // MARK: Lifecycle

    /// playhead-6boz: lightweight initializer. Records the target sqlite
    /// path only; defers `createDirectory`, `sqlite3_open_v2`, the Data
    /// Protection setattr dance, and schema DDL to the first `ensureOpen()`
    /// call (driven by any public method, or by an explicit
    /// `await store.migrate()` / `await store.awaitReady()` from the
    /// caller).
    ///
    /// Why lazy: `PlayheadRuntime.init` runs synchronously from
    /// `PlayheadApp`'s init and extends the launch-storyboard window —
    /// every byte of work in this constructor blocks the splash defense
    /// that lives in SwiftUI-land (see jncn/jndk/hkn1 for the cohort of
    /// fixes this completes). Moving open + DDL to first-use lands the
    /// expense inside a deferred Task, off the main thread, after the
    /// app's launch screen has already given way to a real surface.
    ///
    /// Source-canary
    /// (`AnalysisStoreInitLazinessSourceCanaryTests`) pins this body
    /// against accidentally re-introducing `sqlite3_open_*`,
    /// `FileManager.default.createDirectory`, or `try .write(` calls.
    init(directory: URL? = nil) throws {
        let dir = directory ?? Self.defaultDirectory()
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        self.containerDirectoryToCreate = dir
        self.databaseURL = dbURL
        self.sqliteOpenPath = dbURL.path
    }

    /// Lightweight initializer for an explicit SQLite path, including
    /// `:memory:` for ephemeral in-memory databases (useful in unit
    /// tests). The on-disk handle is opened lazily — see `init(directory:)`.
    init(path: String) throws {
        // `:memory:` is a special SQLite URI handled by the C API
        // directly (sqlite3_open_v2). `databaseURL` is metadata only;
        // the DB opens via the literal path string preserved in
        // `sqliteOpenPath`.
        self.containerDirectoryToCreate = nil
        self.databaseURL = URL(fileURLWithPath: path)
        self.sqliteOpenPath = path
    }

    /// Convenience factory that returns a lazily-initialized store and
    /// eagerly runs migrations in one call. The eager `migrate()` here
    /// is preserved for callers that want a fully-bootstrapped handle —
    /// production launch paths now skip this and let `ensureOpen()` fire
    /// at first use.
    static func open(directory: URL? = nil) async throws -> AnalysisStore {
        let store = try AnalysisStore(directory: directory)
        try await store.migrate()
        return store
    }

    /// playhead-6boz: schema-readiness signal for callers that should
    /// treat an unopened store as "no data yet" rather than block. UI
    /// providers (`ActivitySnapshotProvider`) read this to early-return
    /// an empty list — opening DDL inside a `@MainActor` refresh is the
    /// hkn1 freeze pattern this rail prevents.
    var isOpen: Bool { didOpen }

    /// playhead-6boz: schema-readiness signal for callers that genuinely
    /// need to block until DDL is ready (`BackfillJobRunner` runs on a
    /// background actor; waiting is fine). Idempotent: subsequent calls
    /// observe the already-open handle and short-circuit.
    func awaitReady() async throws {
        try ensureOpen()
    }

    /// playhead-6boz: idempotent first-use bootstrap. Creates the
    /// container directory, opens `sqlite3`, applies the
    /// `.completeUntilFirstUserAuthentication` Data Protection class,
    /// configures pragmas, and runs schema migration. After a successful
    /// pass `didOpen` flips to `true` and the body short-circuits on
    /// every subsequent call. All public actor methods funnel through
    /// this guard (via `exec`/`prepare` and explicit `try ensureOpen()`
    /// at the public-entry layer) so a caller racing `PlayheadRuntime`'s
    /// deferred warmup never observes a half-built database.
    ///
    /// Re-entrancy: `runSchemaMigration()` calls `exec` / `prepare`,
    /// which themselves call `ensureOpen()`. To break the cycle we flip
    /// `didOpen = true` BEFORE invoking `runSchemaMigration` and roll
    /// it back on error. This is safe because by that point the SQLite
    /// handle has been opened, so the inner `exec` calls just observe
    /// `didOpen == true` and pass straight through.
    ///
    /// On migration failure the handle is closed and `db` reset so a
    /// subsequent `ensureOpen()` retries from the top. Production
    /// callers should not retry — a DDL failure on the analysis store
    /// is fatal for the pre-analysis pipeline — but tests that
    /// intentionally seed a corrupt DB and then recover are supported.
    private func ensureOpen() throws {
        if didOpen { return }

        // Step 1: directory + Data Protection. The path-form initializer
        // sets `containerDirectoryToCreate` to `nil` so this step is a
        // no-op for `:memory:` and raw-path test stores.
        if let dir = containerDirectoryToCreate {
            let fm = FileManager.default
            if !fm.fileExists(atPath: dir.path) {
                try fm.createDirectory(at: dir, withIntermediateDirectories: true)
            }
            // Why `.completeUntilFirstUserAuthentication` rather than
            // `.complete`: the app is woken by `BGProcessingTask` and
            // `BGAppRefreshTask` while the device may still be locked.
            // `.complete` renders the SQLite file unreadable in that
            // window, which previously triggered a 4× repro'd crash
            // chain in `PlayheadRuntime.init` (open fails → retry fails
            // → `try!` on the tmp fallback traps).
            // `.completeUntilFirstUserAuthentication` keeps the file
            // protected while the device is at rest pre-unlock and
            // makes it accessible for the rest of the boot session once
            // the user has authenticated at least once, which is the
            // envelope every BGTask runs in.
            // Applied unconditionally (not only on first create) so
            // existing `.complete`-protected installs get migrated to
            // the new class on the next launch.
            try? fm.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: dir.path
            )
        }

        // Step 2: open the SQLite handle.
        var handle: OpaquePointer?
        // NOMUTEX: the enclosing actor already serializes all access,
        // so the full-mutex threading mode is redundant overhead.
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX
        let rc = sqlite3_open_v2(sqliteOpenPath, &handle, flags, nil)
        guard rc == SQLITE_OK, let handle else {
            let msg = handle.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "unknown"
            if let handle { sqlite3_close_v2(handle) }
            throw AnalysisStoreError.openFailed(code: rc, message: msg)
        }
        self.db = handle

        // Step 3: re-stamp the file's protection class. On installs that
        // predate `.completeUntilFirstUserAuthentication` the file
        // inherited `.complete` from the dir at creation time, so we
        // re-stamp it alongside the dir on every `ensureOpen`.
        if containerDirectoryToCreate != nil {
            try? FileManager.default.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: databaseURL.path
            )
        }

        // Step 4: schema migration (pragmas, CREATE TABLE, V*IfNeeded
        // ladder). Flip `didOpen = true` BEFORE invoking the migration
        // so the inner `exec` / `prepare` re-entrancy guards
        // short-circuit. On any thrown error, roll `didOpen` back to
        // false and close the handle so a retry path observes a clean
        // slate. `runSchemaMigration` is internally transactional with
        // ROLLBACK on error so the file itself is left consistent.
        didOpen = true
        do {
            try runSchemaMigration()
        } catch {
            didOpen = false
            if let h = self.db {
                sqlite3_close_v2(h)
                self.db = nil
            }
            throw error
        }
    }

    /// Tracks which database paths have already been migrated in this process
    /// to avoid redundant DDL work on repeated `open()` calls.
    private static let migratedLock = NSLock()
    nonisolated(unsafe) private static var migratedPaths: Set<String> = []

    /// Public migration entry point. Idempotent. After playhead-6boz this
    /// is a thin shim that funnels through the lazy `ensureOpen()` path —
    /// the open + pragmas + DDL all run together on first call. Production
    /// callers (`PlayheadRuntime`'s deferred init Task) `await` this once
    /// off-main so the first hot-path query sees an already-bootstrapped
    /// store. Tests and re-entrant callers are safe; subsequent calls
    /// observe `didOpen == true` and short-circuit.
    func migrate() throws {
        try ensureOpen()
    }

    /// playhead-6boz: the body of the original `migrate()`. Run pragmas
    /// and create all tables / indexes / FTS triggers. Safe to call more
    /// than once. Pragmas are always (re)applied since they live on the
    /// per-connection state, not on the database file. The schema DDL
    /// itself only runs once per database path per process — every DDL
    /// statement uses IF NOT EXISTS so re-running is correct, just
    /// unnecessary work.
    ///
    /// Privately invoked from `ensureOpen()` only. Callers MUST NOT call
    /// this directly — `ensureOpen()` is the gate that guarantees the
    /// SQLite handle has been created and that `didOpen` is set after a
    /// clean run.
    private func runSchemaMigration() throws {
        // M4 fix: pragmas must be applied to *every* connection. The previous
        // implementation short-circuited the entire migrate() call when the
        // path had already been seen, leaving second-instance connections with
        // foreign_keys=OFF, the default journal mode, and no busy_timeout.
        try configurePragmas()

        // C-1: hold the migration lock for the entire body, NOT just the
        // cache check. Two stores opening the same path concurrently used
        // to race: the loser skipped migration on a partially-built DB.
        //
        // C-1 (part 2): validate that the sqlite file on disk still exists
        // before trusting the cache. If the file was deleted (test teardown,
        // user clearing Library/Caches, etc.), drop the stale cache entry
        // and re-run migration against the fresh file. Previously the
        // cache short-circuited and returned a store whose tables did not
        // exist, blowing up on the first query.
        //
        // C-1 (part 3): mark `migratedPaths` only AFTER a successful COMMIT.
        // On any rollback path the path stays out of the cache so a retry
        // re-runs migration rather than silently accepting a half-built DB.
        let path = databaseURL.path
        Self.migratedLock.lock()
        defer { Self.migratedLock.unlock() }

        // The sqlite file may have been deleted out from under the static
        // cache since the last open on this path (test cleanup, user
        // clearing Library/Caches, etc.). `sqlite3_open_v2` with
        // `SQLITE_OPEN_CREATE` will have just recreated it as an empty
        // database, so we can't trust the filesystem presence check alone.
        // Instead, probe for the `_meta` table that `createTables()` always
        // builds: its absence means this connection is looking at a fresh
        // DB that still needs migration, regardless of the cache.
        if Self.migratedPaths.contains(path) {
            if try tableExists("_meta") {
                return
            }
            Self.migratedPaths.remove(path)
        }

        // H-5: wrap the whole migrate body in BEGIN IMMEDIATE … COMMIT so
        // a crash mid-migration cannot leave DDL applied without the
        // matching _meta schema_version row. SQLite supports transactional
        // DDL, so table creation, ALTER TABLE, and the version write all
        // roll back together on any thrown error.
        try exec("BEGIN IMMEDIATE")
        do {
            try createTables()
            // Ordering: transcript_chunks Phase 1 runs before the V*IfNeeded ladder because no later migration touches `transcript_chunks` or `transcript_chunks_fts`, so its FTS rebuild cannot be undone downstream; the backfill only depends on columns `createTables()` has already (re)asserted.
            try migrateTranscriptChunksPhase1()
            try writeInitialSchemaVersionIfNeeded()
            try migrateEvidenceEventsNaturalKeyV2IfNeeded()
            try migrateEvidenceEventsTranscriptVersionV3IfNeeded()
            try migrateAnalysisSessionsShadowRetryV4IfNeeded()
            try migratePodcastPlannerStateV4IfNeeded()
            try migrateAdWindowsPhase6PrepV5IfNeeded()
            try migrateCorrectionEventsV6IfNeeded()
            // Cycle 8 reconciliation: both C4 (Rev3-M5 shadow/targeted) and
            // B6 (Rev3-M6 BackfillJobPhase.rawValue) added a `phase` column
            // for different semantic dimensions. Keep C4's `phase` column
            // (will be renamed to `runMode` in a follow-up reconciliation
            // commit) and introduce a distinct `jobPhase` column for B6.
            //
            // V2 and V3 rebuild `evidence_events` from scratch (CREATE _vN,
            // copy, DROP, RENAME), and those rebuilds intentionally don't
            // carry either column. Re-apply both here once the table has
            // reached its final v3 shape.
            try addColumnIfNeeded(
                table: "evidence_events",
                column: "runMode",
                definition: "TEXT NOT NULL DEFAULT 'shadow'"
            )
            try addColumnIfNeeded(
                table: "evidence_events",
                column: "jobPhase",
                definition: "TEXT NOT NULL DEFAULT 'shadow'"
            )
            try addColumnIfNeeded(
                table: "semantic_scan_results",
                column: "jobPhase",
                definition: "TEXT NOT NULL DEFAULT 'shadow'"
            )
            try migrateSponsorKnowledgeV7IfNeeded()
            // ef2.3.1: CorrectionAttribution columns — unconditional addColumnIfNeeded
            // so existing v6/v7 databases get the new columns too.
            try addColumnIfNeeded(table: "correction_events", column: "correctionType", definition: "TEXT")
            try addColumnIfNeeded(table: "correction_events", column: "causalSource", definition: "TEXT")
            try addColumnIfNeeded(table: "correction_events", column: "targetRefsJSON", definition: "TEXT")
            try migrateFingerprintStoreV8IfNeeded()
            try migrateBoundaryPriorsV9IfNeeded()
            try migrateBracketTrustV10IfNeeded()
            try addColumnIfNeeded(
                table: "feature_windows",
                column: "speakerChangeProxyScore",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: "feature_windows",
                column: "musicBedChangeScore",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            // B10: Fingerprint full-span recovery fields.
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "spanStartOffset",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "spanEndOffset",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "spanDurationSeconds",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "canonicalSponsorEntity",
                definition: "TEXT"
            )
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "anchorLandmarks",
                definition: "TEXT"
            )
            // playhead-ef2.1.4: explanation trace column on decision_events.
            // Guard: decision_events is created in migrateAdWindowsPhase6PrepV5IfNeeded(),
            // which is skipped when a test seeds at v5. The column will be added on the
            // next migrate() after the table exists.
            if try tableExists("decision_events") {
                try addColumnIfNeeded(
                    table: "decision_events",
                    column: "explanationJSON",
                    definition: "TEXT"
                )
            }
            try migrateSourceDemotionsV11IfNeeded()
            try migrateImplicitFeedbackV12IfNeeded()
            // playhead-narl.2: FM dual-run shadow capture storage.
            try migrateShadowFMResponsesV13IfNeeded()
            // playhead-gtt9.8: terminalReason column on analysis_assets.
            try migrateTerminalReasonV14IfNeeded()
            // playhead-i9dj: human-readable show + episode titles so an
            // exported analysis.sqlite is legible without joining to the
            // SwiftData side. Adds `analysis_assets.episodeTitle` and
            // `podcast_profiles.title` (both nullable; lazily populated).
            try migrateSelfDescribingTitlesV15IfNeeded()
            // playhead-4my.10.1: training_examples table — durable
            // snapshot of materialized training rows that survives
            // future cohort prunes. See `migrateTrainingExamplesV16IfNeeded`
            // for the table layout and rationale.
            try migrateTrainingExamplesV16IfNeeded()
            // playhead-4my.10.1 (cycle-2 M-A): rebuild `training_examples`
            // with the post-cycle-1 shape (FK RESTRICT, nullable
            // decisionCohortJSON) for any DB that already opened at v16.
            try migrateTrainingExamplesV17IfNeeded()
            // ef2.5.1: ShowTraitProfile JSON on podcast_profiles.
            try addColumnIfNeeded(
                table: "podcast_profiles",
                column: "traitProfileJSON",
                definition: "TEXT"
            )
            // playhead-7mq: model/policy/feature-schema version columns on
            // the six tables whose row validity depends on model, policy,
            // or feature-schema versions. Foundation for B4 fast
            // revalidation (playhead-zx6i).
            //
            // Sentinels: pre-existing rows are backfilled via SQLite's
            // `ALTER TABLE ADD COLUMN ... DEFAULT <sentinel>` semantics,
            // which applies the default value to every existing row.
            //   - model_version           TEXT NOT NULL DEFAULT 'pre-instrumentation'
            //   - policy_version          INTEGER NOT NULL DEFAULT 0
            //   - feature_schema_version  INTEGER NOT NULL DEFAULT 0
            //
            // Placement: columns are appended to the END of each table
            // (SQLite's only supported position for ADD COLUMN). Three
            // `SELECT * FROM {transcript_chunks, ad_windows, skip_cues}`
            // readers in this file (lines ~2610/2643, ~2747, ~2993) use
            // positional column indices 0..N-1; appending at index N,
            // N+1, N+2 leaves them correct. A test
            // (`AnalysisStoreVersionColumnsMigrationTests.selectStarReadersTolerateNewColumns`)
            // locks that contract.
            //
            // Rollback: `ADD COLUMN` is additive-only. There is no
            // destructive rollback path. A schema-version downgrade
            // leaves the columns in place but unread; existing INSERT
            // paths all use explicit column lists and so continue to
            // work unchanged.
            //
            // Indexes: intentionally NONE. Default per bead spec is no
            // index on these columns; revisit only if profiling under
            // revalidation load (playhead-zx6i) proves need.
            try addModelPolicyFeatureSchemaVersionColumnsIfNeeded()
            // playhead-h7r: tag every analysis_assets row with an
            // `artifact_class`. See `addArtifactClassColumnIfNeeded()` for
            // documentation of sentinel and eviction semantics.
            try addArtifactClassColumnIfNeeded()
            // playhead-uzdq: per-episode execution lease on analysis_jobs
            // (two new columns — no separate lease table) plus the
            // append-only work_journal table and scheduler_epoch
            // singleton in _meta.
            try addEpisodeExecutionLeaseColumnsIfNeeded()
            try createWorkJournalTableIfNeeded()
            try seedSchedulerEpochIfNeeded()
            // playhead-gtt9.1.1: persist episode duration on the
            // `analysis_assets` row at spool time so the coverage guard
            // has a durable denominator on resume-from-backfill paths
            // where `activeShards` is never rehydrated. See
            // `addEpisodeDurationColumnIfNeeded()` for rationale.
            try addEpisodeDurationColumnIfNeeded()
            // playhead-epfk: per-window `AdCatalogStore` top-match
            // similarity. Nullable REAL — `nil` means "store was not
            // wired or no fingerprint was queryable." Existing rows on
            // upgraded DBs get NULL via SQLite's ADD COLUMN default.
            try addColumnIfNeeded(
                table: "ad_windows",
                column: "catalogStoreMatchSimilarity",
                definition: "REAL"
            )
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }

        // Only mark as migrated after a successful COMMIT.
        Self.migratedPaths.insert(path)
    }

    #if DEBUG
    /// H-3: test-only helper that clears the process-global `migratedPaths`
    /// cache. Invoke from test setup when constructing temp-dir stores to
    /// prevent long test runs from accumulating stale entries. Not for
    /// production use — H3-2 gates this behind `#if DEBUG` so release
    /// builds cannot accidentally invalidate the migration cache.
    static func resetMigratedPathsForTesting() {
        migratedLock.withLock {
            migratedPaths.removeAll()
        }
    }

    /// Cycle 4 H1: runs ONLY the V*IfNeeded migration ladder against an
    /// already-opened store, bypassing `createTables()`. The cycle-2
    /// `MigrationLadderTests` seeded `_meta.schema_version` but still
    /// went through full `migrate()`, which calls `createTables()` first
    /// and builds every table in its current shape via
    /// `CREATE TABLE IF NOT EXISTS`. Tables-already-present short-circuits
    /// most of the ladder body, so the tests passed even against pre-C6
    /// code (the C6 bug could not actually be reached).
    ///
    /// This seam lets a test seed a v1-shape DB manually (via raw SQL)
    /// and then run the ladder without painting over. Failing under
    /// pre-C6 code proves the rail bites.
    ///
    /// Not transaction-wrapped — tests are expected to begin/commit
    /// themselves when they want to assert rollback semantics. The
    /// default behavior here mirrors what `migrate()` would do minus
    /// `createTables()`.
    func migrateOnlyForTesting() throws {
        try writeInitialSchemaVersionIfNeeded()
        if try tableExists("transcript_chunks") {
            try migrateTranscriptChunksPhase1()
        }
        try migrateEvidenceEventsNaturalKeyV2IfNeeded()
        try migrateEvidenceEventsTranscriptVersionV3IfNeeded()
        try migrateAnalysisSessionsShadowRetryV4IfNeeded()
        try migratePodcastPlannerStateV4IfNeeded()
        try migrateAdWindowsPhase6PrepV5IfNeeded()
        try migrateCorrectionEventsV6IfNeeded()
        try migrateSponsorKnowledgeV7IfNeeded()
        // ef2.3.1: CorrectionAttribution columns — unconditional addColumnIfNeeded
        // so existing v6/v7 databases get the new columns too.
        try addColumnIfNeeded(table: "correction_events", column: "correctionType", definition: "TEXT")
        try addColumnIfNeeded(table: "correction_events", column: "causalSource", definition: "TEXT")
        try addColumnIfNeeded(table: "correction_events", column: "targetRefsJSON", definition: "TEXT")
        try migrateFingerprintStoreV8IfNeeded()
        try migrateBoundaryPriorsV9IfNeeded()
        try migrateBracketTrustV10IfNeeded()
        try migrateSourceDemotionsV11IfNeeded()
        try migrateImplicitFeedbackV12IfNeeded()
        // playhead-narl.2: shadow capture storage — must also be applied in the
        // ladder-only test seam so migration-ladder tests see the new table.
        try migrateShadowFMResponsesV13IfNeeded()
        // playhead-gtt9.8: terminalReason column — the ladder-only seam
        // must also apply the migration so schema-version tests lock the
        // upgrade at v14. Guarded by tableExists because some seeded
        // fixtures may omit analysis_assets.
        if try tableExists("analysis_assets") {
            try migrateTerminalReasonV14IfNeeded()
        }
        // playhead-i9dj: episodeTitle on analysis_assets, title on
        // podcast_profiles. Ladder-only seam mirrors `migrate()` so
        // schema-version tests lock at v15. Each branch is guarded by
        // `tableExists` because seeded fixtures may omit either table.
        let assetsExist = try tableExists("analysis_assets")
        let profilesExist = try tableExists("podcast_profiles")
        if assetsExist || profilesExist {
            try migrateSelfDescribingTitlesV15IfNeeded()
        }
        // playhead-4my.10.1: training_examples — ladder-only seam mirrors
        // `migrate()` so schema-version tests lock at v17. The table has
        // no dependencies on legacy seeded fixtures so we can apply
        // unconditionally. cycle-2 M-A bumps to v17 to rebuild any
        // pre-fix v16 DB into the corrected shape.
        try migrateTrainingExamplesV16IfNeeded()
        try migrateTrainingExamplesV17IfNeeded()
        // H1 fix: mirror the addColumnIfNeeded calls from migrate() that
        // follow the versioned ladder steps. Without these, the isolated-
        // ladder test seam cannot catch regressions in column additions.
        if try tableExists("semantic_scan_results") {
            try addColumnIfNeeded(
                table: "semantic_scan_results",
                column: "jobPhase",
                definition: "TEXT NOT NULL DEFAULT 'shadow'"
            )
        }
        // correction_events columns already added above (lines 720-722).
        // B10: Fingerprint full-span recovery fields.
        if try tableExists("ad_copy_fingerprints") {
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "spanStartOffset",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "spanEndOffset",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "spanDurationSeconds",
                definition: "REAL NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "canonicalSponsorEntity",
                definition: "TEXT"
            )
            try addColumnIfNeeded(
                table: "ad_copy_fingerprints",
                column: "anchorLandmarks",
                definition: "TEXT"
            )
        }
        // playhead-ef2.1.4: explanation trace column on decision_events.
        if try tableExists("decision_events") {
            try addColumnIfNeeded(
                table: "decision_events",
                column: "explanationJSON",
                definition: "TEXT"
            )
        }
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_windows (
                analysisAssetId   TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startTime         REAL NOT NULL,
                endTime           REAL NOT NULL,
                rms               REAL NOT NULL,
                spectralFlux      REAL NOT NULL,
                musicProbability  REAL NOT NULL,
                speakerChangeProxyScore REAL NOT NULL DEFAULT 0,
                musicBedChangeScore REAL NOT NULL DEFAULT 0,
                musicBedOnsetScore REAL NOT NULL DEFAULT 0,
                musicBedOffsetScore REAL NOT NULL DEFAULT 0,
                musicBedLevelRaw  TEXT NOT NULL DEFAULT 'none',
                pauseProbability  REAL NOT NULL,
                speakerClusterId  INTEGER,
                jingleHash        TEXT,
                featureVersion    INTEGER NOT NULL,
                PRIMARY KEY (analysisAssetId, startTime)
            )
            """)
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_extraction_state (
                analysisAssetId TEXT PRIMARY KEY REFERENCES analysis_assets(id) ON DELETE CASCADE,
                lastWindowStartTime REAL NOT NULL,
                lastWindowEndTime REAL NOT NULL,
                lastRms REAL NOT NULL,
                lastMusicProbability REAL NOT NULL,
                lastRawSpeakerChangeProxyScore REAL NOT NULL,
                penultimateRawSpeakerChangeProxyScore REAL,
                lastMagnitudesJSON TEXT NOT NULL,
                featureVersion INTEGER NOT NULL
            )
            """)
        try addColumnIfNeeded(
            table: "feature_windows",
            column: "speakerChangeProxyScore",
            definition: "REAL NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "feature_windows",
            column: "musicBedChangeScore",
            definition: "REAL NOT NULL DEFAULT 0"
        )
        // musicBedOnsetScore, musicBedOffsetScore, musicBedLevelRaw are
        // already defined in the CREATE TABLE above — no addColumnIfNeeded needed.
        // Mirror the belt-and-suspenders phase/jobPhase column re-adds
        // that `migrate()` performs after the v2/v3 evidence_events
        // rebuild (cycle-8 reconciliation: both columns coexist).
        try addColumnIfNeeded(
            table: "evidence_events",
            column: "runMode",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try addColumnIfNeeded(
            table: "evidence_events",
            column: "jobPhase",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        // playhead-7mq: mirror the `addModelPolicyFeatureSchemaVersionColumnsIfNeeded`
        // call from `migrate()` so the isolated ladder test seam also
        // exercises the new columns. Guarded by tableExists() because
        // `migrateOnlyForTesting()` intentionally skips `createTables()`
        // and some seeded fixtures may not include every in-scope table.
        try addModelPolicyFeatureSchemaVersionColumnsIfNeededForExistingTables()
        // playhead-h7r: mirror the `addArtifactClassColumnIfNeeded` call
        // from `migrate()`. Guarded by `tableExists` because some seeded
        // fixtures may not include `analysis_assets`.
        if try tableExists("analysis_assets") {
            try addArtifactClassColumnIfNeeded()
        }
        // playhead-uzdq: mirror the lease + work_journal additions.
        // Guarded on `analysis_jobs` / `_meta` so fixtures without
        // those tables don't error out; production callers already
        // have them via `createTables()`.
        if try tableExists("analysis_jobs") {
            try addEpisodeExecutionLeaseColumnsIfNeeded()
        }
        if try tableExists("_meta") {
            try createWorkJournalTableIfNeeded()
            try seedSchedulerEpochIfNeeded()
        }
        // playhead-gtt9.1.1: mirror the `addEpisodeDurationColumnIfNeeded`
        // call from `migrate()`. Guarded by `tableExists` because some
        // seeded fixtures may not include `analysis_assets`.
        if try tableExists("analysis_assets") {
            try addEpisodeDurationColumnIfNeeded()
        }
    }
    #endif

    /// playhead-7mq: variant of
    /// `addModelPolicyFeatureSchemaVersionColumnsIfNeeded` that only
    /// touches tables that actually exist. Used by
    /// `migrateOnlyForTesting()` where fixtures may omit some tables.
    /// Production callers use the unguarded helper because
    /// `createTables()` has already built every table.
    private func addModelPolicyFeatureSchemaVersionColumnsIfNeededForExistingTables() throws {
        let inScopeTables = [
            "analysis_sessions",
            "transcript_chunks",
            "feature_windows",
            "feature_extraction_state",
            "ad_windows",
            "skip_cues",
        ]
        for table in inScopeTables where try tableExists(table) {
            try addColumnIfNeeded(
                table: table,
                column: "model_version",
                definition: "TEXT NOT NULL DEFAULT 'pre-instrumentation'"
            )
            try addColumnIfNeeded(
                table: table,
                column: "policy_version",
                definition: "INTEGER NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: table,
                column: "feature_schema_version",
                definition: "INTEGER NOT NULL DEFAULT 0"
            )
        }
    }

    /// Probes `sqlite_master` for a table by name. Used by `migrate()` to
    /// detect a stale `migratedPaths` cache entry pointing at a file that
    /// has since been deleted and recreated empty.
    private func tableExists(_ table: String) throws -> Bool {
        let stmt = try prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '\(table)'")
        defer { sqlite3_finalize(stmt) }
        return sqlite3_step(stmt) == SQLITE_ROW
    }

    private func migrateTranscriptChunksPhase1() throws {
        // Add columns for transcript identity. The old implementation parsed
        // the SQLite error string for "duplicate column name"; we now check
        // PRAGMA table_info first and only ALTER when the column is missing.
        try addColumnIfNeeded(table: "transcript_chunks", column: "transcriptVersion", definition: "TEXT")
        try addColumnIfNeeded(table: "transcript_chunks", column: "atomOrdinal", definition: "INTEGER")
        try addColumnIfNeeded(table: "transcript_chunks", column: "weakAnchorMetadataJSON", definition: "TEXT")
        // B7: speaker label from validated ASR diarization (nil until available).
        try addColumnIfNeeded(table: "transcript_chunks", column: "speakerId", definition: "INTEGER")
        try backfillLegacyTranscriptChunksPhase1IfNeeded()
    }

    private func backfillLegacyTranscriptChunksPhase1IfNeeded() throws {
        // invariant: no nested transaction/savepoint is required here.
        // The production caller (`migrate()`) runs this inside an outer
        // BEGIN IMMEDIATE … COMMIT, and the test-only caller
        // (`migrateOnlyForTesting()`) is intentionally unwrapped. Even
        // without the outer transaction, re-running after a partial crash
        // is correct because (a) the SELECT predicate
        // `WHERE pass != 'fast' AND (transcriptVersion IS NULL OR
        // atomOrdinal IS NULL)` self-skips rows already backfilled, and
        // (b) `legacyTranscriptVersion` is a SHA256 over the chunks'
        // normalizedText — content-addressed, so the hash is stable
        // across partial states and a resumed run writes the same value
        // that a crashed run would have written.
        let assetStmt = try prepare("""
            SELECT DISTINCT analysisAssetId
            FROM transcript_chunks
            WHERE pass != 'fast'
              AND (transcriptVersion IS NULL OR atomOrdinal IS NULL)
            ORDER BY analysisAssetId
            """)
        defer { sqlite3_finalize(assetStmt) }

        let updateStmt = try prepare("""
            UPDATE transcript_chunks
            SET transcriptVersion = ?, atomOrdinal = ?
            WHERE id = ?
            """)
        defer { sqlite3_finalize(updateStmt) }

        var rebuiltFTS = false
        while sqlite3_step(assetStmt) == SQLITE_ROW {
            let assetId = text(assetStmt, 0)
            if !rebuiltFTS, try tableExists("transcript_chunks_fts") {
                // Old databases can contain transcript rows that predate the
                // external-content FTS table. Rebuild before mutating any of
                // those rows so the UPDATE trigger's delete/insert cycle sees
                // matching index entries instead of tripping SQLite corruption
                // checks on missing rowids.
                try exec("INSERT INTO transcript_chunks_fts(transcript_chunks_fts) VALUES('rebuild')")
                rebuiltFTS = true
            }
            let chunks = try fetchTranscriptChunks(assetId: assetId)
            let legacyChunks = chunks
                .filter { $0.pass != "fast" }
                .sorted(by: legacyTranscriptChunkSort)
            guard !legacyChunks.isEmpty else { continue }

            let version = legacyTranscriptVersion(for: legacyChunks)
            for (ordinal, chunk) in legacyChunks.enumerated() {
                sqlite3_reset(updateStmt)
                bind(updateStmt, 1, version)
                bind(updateStmt, 2, ordinal)
                bind(updateStmt, 3, chunk.id)
                try step(updateStmt, expecting: SQLITE_DONE)
            }
        }
    }

    private func legacyTranscriptChunkSort(_ lhs: TranscriptChunk, _ rhs: TranscriptChunk) -> Bool {
        if lhs.chunkIndex != rhs.chunkIndex {
            return lhs.chunkIndex < rhs.chunkIndex
        }
        return lhs.id < rhs.id
    }

    private func legacyTranscriptVersion(for chunks: [TranscriptChunk]) -> String {
        var hasher = SHA256()
        for chunk in chunks.sorted(by: legacyTranscriptChunkSort) {
            let textData = Data(chunk.normalizedText.utf8)
            withUnsafeBytes(of: UInt32(textData.count).bigEndian) { hasher.update(bufferPointer: $0) }
            hasher.update(data: textData)
        }
        return hasher.finalize().prefix(16).map { String(format: "%02x", $0) }.joined()
    }

    private func columnExists(table: String, column: String) throws -> Bool {
        // PRAGMA table_info(...) cannot be parameterized via bind, so the
        // table name is interpolated. Both arguments are in-process constants
        // (no user input), so SQL injection is not in scope here.
        let sql = "PRAGMA table_info(\(table))"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        while sqlite3_step(stmt) == SQLITE_ROW {
            // Column 1 of table_info is the column name.
            let name = optionalText(stmt, 1) ?? ""
            if name == column { return true }
        }
        return false
    }

    private func addColumnIfNeeded(table: String, column: String, definition: String) throws {
        if try columnExists(table: table, column: column) {
            return
        }
        try exec("ALTER TABLE \(table) ADD COLUMN \(column) \(definition)")
    }

    /// playhead-7mq: add `model_version`, `policy_version`, and
    /// `feature_schema_version` columns to each of the six tables
    /// whose row validity depends on model / policy / feature-schema
    /// versions (the foundation for B4 fast revalidation in
    /// playhead-zx6i). Idempotent — re-running is a no-op.
    ///
    /// Pre-existing rows are backfilled with sentinel values via the
    /// `DEFAULT` clause on `ALTER TABLE ADD COLUMN`:
    ///   - `model_version`           → `'pre-instrumentation'`
    ///   - `policy_version`          → `0`
    ///   - `feature_schema_version`  → `0`
    ///
    /// B4 revalidation logic (OUT of scope here, implemented in
    /// playhead-zx6i) treats these sentinels as "always revalidate".
    ///
    /// Invariant: columns are NOT NULL with a default; the default
    /// backfills existing rows, and future INSERTs that omit the
    /// columns (all existing call-sites do) inherit the default.
    /// Call-sites that want explicit versions will add bind positions
    /// when playhead-zx6i wires revalidation.
    private func addModelPolicyFeatureSchemaVersionColumnsIfNeeded() throws {
        let inScopeTables = [
            "analysis_sessions",
            "transcript_chunks",
            "feature_windows",
            "feature_extraction_state",
            "ad_windows",
            "skip_cues",
        ]
        for table in inScopeTables {
            try addColumnIfNeeded(
                table: table,
                column: "model_version",
                definition: "TEXT NOT NULL DEFAULT 'pre-instrumentation'"
            )
            try addColumnIfNeeded(
                table: table,
                column: "policy_version",
                definition: "INTEGER NOT NULL DEFAULT 0"
            )
            try addColumnIfNeeded(
                table: table,
                column: "feature_schema_version",
                definition: "INTEGER NOT NULL DEFAULT 0"
            )
        }
    }

    /// playhead-h7r: add `artifact_class` to `analysis_assets`. Idempotent.
    ///
    /// New artifacts are classified at insert time into one of three
    /// classes (see ``ArtifactClass``) so the storage-budget enforcer
    /// (``StorageBudget``) can apply per-class LRU eviction and the
    /// warm-resume-bundle ratio invariant.
    ///
    /// Sentinel: legacy rows without an explicit class are migrated to
    /// `'media'` via SQLite's `ALTER TABLE ADD COLUMN ... DEFAULT`
    /// semantics. `'media'` is the safe default because it is the only
    /// evictable class under dual-cap pressure — misclassifying a legacy
    /// row as media does not leak bytes, it just makes that row a
    /// candidate for eviction ahead of correctly-tagged warm-resume
    /// bundles.
    ///
    /// Placement: the column is appended to the END of `analysis_assets`
    /// (SQLite's only supported `ADD COLUMN` position). Existing
    /// `SELECT *` readers in this file (`readAsset`, at the positional
    /// indices 0..11, where index 11 is `createdAt`) remain correct
    /// because the new column sits at index 12; a dedicated fetch path
    /// reads it out when needed.
    ///
    /// Rollback: `ADD COLUMN` is additive-only. Existing INSERT call-sites
    /// use explicit column lists that now include `artifact_class`, so a
    /// schema downgrade that drops the column would break inserts —
    /// there is no destructive rollback path.
    private func addArtifactClassColumnIfNeeded() throws {
        // Cycle-3 hardening (downgraded in cycle-4): the rawValue is
        // interpolated directly into the SQL DEFAULT clause. Today it is
        // the literal `"media"`, which is safe. If a future contributor
        // renames the case to a value containing an apostrophe (e.g.
        // `"podcast's media"`), the unescaped interpolation would produce
        // broken SQL.
        //
        // The primary defense lives in the CI test
        // `artifactClassRawValuesAreSqlSafe` (see
        // `ArtifactClassMigrationTests`), which fails the build at PR
        // time if any rawValue contains a SQL string-literal-breaker.
        // The check below is DEBUG-only defense-in-depth so a contributor
        // running tests locally also catches the regression — it must NOT
        // crash users in Release if the CI gate is somehow bypassed.
        assert(
            !ArtifactClass.media.rawValue.contains("'"),
            "ArtifactClass.media.rawValue must not contain an apostrophe; would break migration SQL. CI test artifactClassRawValuesAreSqlSafe should have caught this earlier."
        )
        try addColumnIfNeeded(
            table: "analysis_assets",
            column: "artifact_class",
            definition: "TEXT NOT NULL DEFAULT '\(ArtifactClass.media.rawValue)'"
        )
    }

    /// playhead-gtt9.1.1: add `episodeDurationSec REAL` to
    /// `analysis_assets`. Idempotent.
    ///
    /// The column captures the total audio duration (sum of shard
    /// durations) computed during `spool`. Coverage-guard denominators
    /// read from this column on resume-from-persisted-`.backfill` paths
    /// where `activeShards` is never rehydrated. See
    /// ``AnalysisCoordinator/resolveEpisodeDuration(activeShards:persistedDuration:)``
    /// for how the value is consumed.
    ///
    /// Sentinel: legacy rows and placeholder rows (inserted before
    /// audio decode) leave the column NULL. The resolver treats NULL
    /// and non-positive values identically — "missing" — which routes
    /// the guards to their fail-safe shortcuts.
    ///
    /// Placement: column is appended at the END of `analysis_assets`
    /// (SQLite's only supported `ADD COLUMN` position). After
    /// `addArtifactClassColumnIfNeeded` placed `artifact_class` at
    /// index 12, `episodeDurationSec` now sits at index 13. `readAsset`
    /// uses an explicit SELECT column list (not `SELECT *`) for the
    /// new column so ordering is robust across migrations.
    ///
    /// Rollback: `ADD COLUMN` is additive-only. No destructive rollback
    /// path.
    private func addEpisodeDurationColumnIfNeeded() throws {
        try addColumnIfNeeded(
            table: "analysis_assets",
            column: "episodeDurationSec",
            definition: "REAL"
        )
    }

    /// playhead-uzdq: per-episode execution lease state on
    /// `analysis_jobs`. Adds two columns — `generationID TEXT NOT NULL
    /// DEFAULT ''` and `schedulerEpoch INTEGER NOT NULL DEFAULT 0` —
    /// using `addColumnIfNeeded` so the migration is idempotent and
    /// pre-existing rows backfill to the NULL-lease sentinel.
    ///
    /// Sentinels:
    /// - `generationID = ''` → no lease has ever been acquired on this
    ///   row. Distinguished from a live lease by `leaseOwner IS NOT NULL`.
    /// - `schedulerEpoch = 0` → conservative floor. Fresh epochs start
    ///   at 1 (`seedSchedulerEpochIfNeeded`) so any real lease has
    ///   epoch >= 1.
    ///
    /// Placement: appended at the END of `analysis_jobs` (SQLite's only
    /// supported ADD COLUMN position). `readJob` reads both columns at
    /// the new trailing indices 21 and 22.
    ///
    /// No index is added — leases are looked up by `episodeId` (already
    /// indexed by `idx_jobs_episode`), and `generationID` comparisons
    /// are always scoped to a single episode so a scan is fine.
    private func addEpisodeExecutionLeaseColumnsIfNeeded() throws {
        try addColumnIfNeeded(
            table: "analysis_jobs",
            column: "generationID",
            definition: "TEXT NOT NULL DEFAULT ''"
        )
        try addColumnIfNeeded(
            table: "analysis_jobs",
            column: "schedulerEpoch",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )
    }

    /// playhead-uzdq: append-only audit trail of lease lifecycle events.
    /// Indexes:
    /// - `idx_wj_episode_gen` drives orphan-recovery's "last event per
    ///   generation" lookup.
    /// - `idx_wj_epoch` supports cold-start reconciliation's epoch sweep
    ///   (drop journal effects with `scheduler_epoch > _meta.scheduler_epoch`).
    ///
    /// Every row is tagged `artifact_class = 'scratch'` (see
    /// ``ArtifactClass``) — journal state is reconstructible audit
    /// metadata, never user-facing media.
    private func createWorkJournalTableIfNeeded() throws {
        try exec("""
            CREATE TABLE IF NOT EXISTS work_journal (
                id TEXT PRIMARY KEY,
                episode_id TEXT NOT NULL,
                generation_id TEXT NOT NULL,
                scheduler_epoch INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                event_type TEXT NOT NULL,
                cause TEXT,
                metadata TEXT NOT NULL DEFAULT '{}',
                artifact_class TEXT NOT NULL DEFAULT '\(ArtifactClass.scratch.rawValue)'
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_wj_episode_gen ON work_journal(episode_id, generation_id)")
        try exec("CREATE INDEX IF NOT EXISTS idx_wj_epoch ON work_journal(scheduler_epoch)")
    }

    /// playhead-uzdq: seed `_meta(key='scheduler_epoch', value='1')` on
    /// a brand-new DB so every lease captures a non-zero epoch. Legacy
    /// rows (pre-uzdq) that carry `schedulerEpoch=0` are the sentinel
    /// for "no lease has ever been acquired" and are distinguishable
    /// from live leases by `leaseOwner IS NOT NULL`.
    ///
    /// `INSERT OR IGNORE` makes this idempotent across re-migration.
    private func seedSchedulerEpochIfNeeded() throws {
        let stmt = try prepare("INSERT OR IGNORE INTO _meta (key, value) VALUES ('scheduler_epoch', '1')")
        defer { sqlite3_finalize(stmt) }
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Seeds `_meta.schema_version = '1'` on a brand-new database so the
    /// subsequent migration ladder (`migrateEvidenceEventsNaturalKeyV2IfNeeded`,
    /// `…V3IfNeeded`, `…V4IfNeeded`, `migratePodcastPlannerStateV4IfNeeded`,
    /// `migrateAdWindowsPhase6PrepV5IfNeeded`)
    /// climbs correctly to `currentSchemaVersion`.
    ///
    /// C6 fix (scope): this used to bind `String(currentSchemaVersion)`
    /// which left brand-new DBs at the latest version immediately,
    /// causing every V*IfNeeded migration's `guard schemaVersion < N` to
    /// short-circuit.
    ///
    /// Important caveat (cycle-4 L4): **in the production `migrate()`
    /// path, `createTables()` runs BEFORE this function** and already
    /// builds every table in its final v4 shape via
    /// `CREATE TABLE IF NOT EXISTS`, so the V*IfNeeded blocks are
    /// effectively prophylactic for production callers — the ladder they
    /// fix cannot be reached from `migrate()` alone, because the tables
    /// they would recreate already exist. The C6 fix matters for any
    /// future migration that does work `createTables()` cannot (e.g. a
    /// data backfill across existing rows, or a DDL change that requires
    /// inspecting `_meta.schema_version`). It also matters for
    /// `migrateOnlyForTesting()`, which bypasses `createTables()` so the
    /// ladder can be exercised against a hand-seeded v1/v2/v3 DB in
    /// isolation — that is the only path where the pre-C6 bug was
    /// actually reachable.
    ///
    /// `INSERT OR IGNORE` keeps this idempotent on re-migration: if a row
    /// already exists (any version) we leave it alone and the V*IfNeeded
    /// blocks read it via `schemaVersion()` and decide what to do.
    private func writeInitialSchemaVersionIfNeeded() throws {
        let stmt = try prepare("INSERT OR IGNORE INTO _meta (key, value) VALUES ('schema_version', '1')")
        defer { sqlite3_finalize(stmt) }
        try step(stmt, expecting: SQLITE_DONE)
    }

    private func setSchemaVersion(_ version: Int) throws {
        let stmt = try prepare("""
            INSERT INTO _meta (key, value) VALUES ('schema_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, String(version))
        try step(stmt, expecting: SQLITE_DONE)
    }

    private func migrateEvidenceEventsNaturalKeyV2IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 2 else { return }

        try exec("""
            CREATE TABLE evidence_events_v2 (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                eventType TEXT NOT NULL,
                sourceType TEXT NOT NULL,
                atomOrdinals TEXT NOT NULL,
                evidenceJSON TEXT NOT NULL,
                scanCohortJSON TEXT NOT NULL,
                createdAt REAL NOT NULL,
                UNIQUE(analysisAssetId, eventType, sourceType, atomOrdinals, evidenceJSON, scanCohortJSON)
            )
            """)
        try exec("""
            INSERT OR IGNORE INTO evidence_events_v2
            (id, analysisAssetId, eventType, sourceType, atomOrdinals, evidenceJSON, scanCohortJSON, createdAt)
            SELECT id, analysisAssetId, eventType, sourceType, atomOrdinals, evidenceJSON, scanCohortJSON, createdAt
            FROM evidence_events
            """)
        try exec("DROP TABLE evidence_events")
        try exec("ALTER TABLE evidence_events_v2 RENAME TO evidence_events")
        try exec("CREATE INDEX IF NOT EXISTS idx_evidence_events_asset_created ON evidence_events(analysisAssetId, createdAt ASC)")
        try setSchemaVersion(2)
    }

    private func migrateEvidenceEventsTranscriptVersionV3IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 3 else { return }

        try exec("""
            CREATE TABLE evidence_events_v3 (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                eventType TEXT NOT NULL,
                sourceType TEXT NOT NULL,
                atomOrdinals TEXT NOT NULL,
                evidenceJSON TEXT NOT NULL,
                scanCohortJSON TEXT NOT NULL,
                transcriptVersion TEXT NOT NULL DEFAULT '',
                createdAt REAL NOT NULL,
                UNIQUE(
                    analysisAssetId, eventType, sourceType, atomOrdinals,
                    evidenceJSON, scanCohortJSON, transcriptVersion
                )
            )
            """)
        try exec("""
            INSERT OR IGNORE INTO evidence_events_v3
            (id, analysisAssetId, eventType, sourceType, atomOrdinals,
             evidenceJSON, scanCohortJSON, transcriptVersion, createdAt)
            SELECT id, analysisAssetId, eventType, sourceType, atomOrdinals,
                   evidenceJSON, scanCohortJSON, '', createdAt
            FROM evidence_events
            """)
        try exec("DROP TABLE evidence_events")
        try exec("ALTER TABLE evidence_events_v3 RENAME TO evidence_events")
        try exec("CREATE INDEX IF NOT EXISTS idx_evidence_events_asset_created ON evidence_events(analysisAssetId, createdAt ASC)")
        try setSchemaVersion(3)
    }

    /// bd-3bz (Phase 4): add `needsShadowRetry` and `shadowRetryPodcastId`
    /// columns to `analysis_sessions`. The Foundation Models shadow phase
    /// stamps these when it bails on `canUseFoundationModels == false`; a
    /// capability observer in `PlayheadRuntime` drains the queue after FM
    /// recovers (see `runShadowFMPhase` and `retryShadowFMPhaseForSession`).
    ///
    /// Idempotent via `columnExists` checks so re-running the migration (or
    /// opening a schema-v3 DB that was manually upgraded) does not fail.
    /// NOT retroactively marked — sessions already in `.complete` stay as-is;
    /// only sessions whose shadow phase bails AFTER the migration set the
    /// flag.
    ///
    /// H10: column was originally `needs_shadow_retry` (snake_case,
    /// inconsistent with the rest of `analysis_sessions`). Renamed in place
    /// in the v4 migration block — single-user app, full DB wipe is
    /// acceptable so no v5 bump. Pre-existing on-device DBs that already
    /// applied v4 with the snake_case column are repaired by the
    /// `renameColumnIfNeeded` call below.
    private func migrateAnalysisSessionsShadowRetryV4IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 4 else {
            // Even on v4+ DBs, repair the H10 rename if a pre-rename column
            // is still present. Idempotent: no-op when the column already
            // has the new name.
            try renameSnakeCaseShadowRetryIfNeeded()
            return
        }
        try addColumnIfNeeded(
            table: "analysis_sessions",
            column: "needsShadowRetry",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "analysis_sessions",
            column: "shadowRetryPodcastId",
            definition: "TEXT"
        )
        // Partial index: cheap lookups for the retry drain. SQLite supports
        // WHERE clauses on indexes since 3.8.0.
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_sessions_shadow_retry
            ON analysis_sessions(id)
            WHERE needsShadowRetry = 1
            """)
        try setSchemaVersion(4)
    }

    /// H10 repair: an earlier v4 migration created the column as
    /// `needs_shadow_retry`. If a pre-rename column is still present and the
    /// new camelCase column is not, rename in place via SQLite's
    /// `ALTER TABLE ... RENAME COLUMN` (3.25+). Idempotent.
    private func renameSnakeCaseShadowRetryIfNeeded() throws {
        let hasNew = try columnExists(table: "analysis_sessions", column: "needsShadowRetry")
        let hasOld = try columnExists(table: "analysis_sessions", column: "needs_shadow_retry")
        if hasNew { return }
        if hasOld {
            // Drop the old partial index first — its WHERE predicate
            // references the old column name and the rename would invalidate
            // the predicate.
            try exec("DROP INDEX IF EXISTS idx_sessions_shadow_retry")
            try exec("ALTER TABLE analysis_sessions RENAME COLUMN needs_shadow_retry TO needsShadowRetry")
            try exec("""
                CREATE INDEX IF NOT EXISTS idx_sessions_shadow_retry
                ON analysis_sessions(id)
                WHERE needsShadowRetry = 1
                """)
        }
    }

    /// bd-m8k: v4 creates `podcast_planner_state` for per-podcast
    /// CoveragePlanner state (observed episode count, episodes since last
    /// full rescan, precision ring, cached stable-precision flag). The table
    /// is created empty — we do NOT backfill rows for existing podcasts.
    /// Rows are upserted lazily the first time a podcast is observed.
    ///
    /// Idempotent: `CREATE TABLE IF NOT EXISTS` is a no-op when the table
    /// already exists (e.g. on a fresh DB that picked up the baseline DDL in
    /// `createTables()` before this migration ran). Guarded by the schema
    /// version so an upgraded DB still executes the DDL once and then never
    /// again.
    ///
    /// Coexists with `migrateAnalysisSessionsShadowRetryV4IfNeeded` (bd-3bz):
    /// both run during the v3→v4 step, both touch independent tables, both
    /// call setSchemaVersion(4) at the end (idempotent).
    private func migratePodcastPlannerStateV4IfNeeded() throws {
        // Cycle 4 B4: two new columns were added in place to the v4 schema
        // (`episodesObservedWithoutSampleCount`,
        // `narrowingAllPhasesEmptyEpisodeCount`). The `addColumnIfNeeded`
        // calls MUST run AFTER the `CREATE TABLE IF NOT EXISTS` below —
        // otherwise `migrateOnlyForTesting` (which skips `createTables()`)
        // hits an `ALTER TABLE` on a non-existent table when climbing the
        // ladder from a v1-shape DB. On a fresh DB both blocks are no-ops.

        let needsV4Upgrade = (try schemaVersion() ?? 1) < 4

        try exec("""
            CREATE TABLE IF NOT EXISTS podcast_planner_state (
                podcastId                                 TEXT PRIMARY KEY,
                observedEpisodeCount                      INTEGER NOT NULL DEFAULT 0,
                episodesSinceLastFullRescan               INTEGER NOT NULL DEFAULT 0,
                stablePrecisionFlag                       INTEGER NOT NULL DEFAULT 0,
                lastFullRescanAt                          REAL,
                precisionSample1                          REAL,
                precisionSample2                          REAL,
                precisionSample3                          REAL,
                precisionSampleCount                      INTEGER NOT NULL DEFAULT 0,
                episodesObservedWithoutSampleCount        INTEGER NOT NULL DEFAULT 0,
                narrowingAllPhasesEmptyEpisodeCount       INTEGER NOT NULL DEFAULT 0
            )
            """)

        // Idempotent column adds for pre-Cycle-4 v4 DBs. On a fresh DB both
        // are no-ops because the CREATE TABLE above already defined them.
        try addColumnIfNeeded(
            table: "podcast_planner_state",
            column: "episodesObservedWithoutSampleCount",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "podcast_planner_state",
            column: "narrowingAllPhasesEmptyEpisodeCount",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )

        if needsV4Upgrade {
            try setSchemaVersion(4)
        }
    }

    /// Called from BOTH `migrate()` and `migrateOnlyForTesting()`. The `schemaVersion() < 5`
    /// guard makes it idempotent: the second call is a no-op. All DDL statements inside
    /// use `IF NOT EXISTS` / `addColumnIfNeeded` which are also idempotent. Any future
    /// non-idempotent step added here MUST be guarded by its own existence check.
    private func migrateAdWindowsPhase6PrepV5IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 5 else { return }

        try addColumnIfNeeded(
            table: "ad_windows",
            column: "evidenceSources",
            definition: "TEXT"
        )
        try addColumnIfNeeded(
            table: "ad_windows",
            column: "eligibilityGate",
            definition: "TEXT"
        )

        // Phase 6 decision tables (playhead-4my.6.3) — same v5 batch
        // UNIQUE on analysisAssetId so INSERT OR REPLACE enforces one active decision per asset.
        // A new cohort recomputes decisions → replaces the old row, preserving the last-writer-wins
        // contract without accumulating stale rows.
        try exec("""
            CREATE TABLE IF NOT EXISTS ad_decision_results (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL UNIQUE,
                decisionCohortJSON  TEXT NOT NULL,
                inputArtifactRefs   TEXT NOT NULL,
                decisionJSON        TEXT NOT NULL,
                createdAt           REAL NOT NULL
            )
            """)

        try exec("""
            CREATE TABLE IF NOT EXISTS decision_events (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL,
                eventType           TEXT NOT NULL,
                windowId            TEXT NOT NULL,
                proposalConfidence  REAL NOT NULL,
                skipConfidence      REAL NOT NULL,
                eligibilityGate     TEXT NOT NULL,
                policyAction        TEXT NOT NULL,
                decisionCohortJSON  TEXT NOT NULL,
                createdAt           REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_de_asset ON decision_events(analysisAssetId)")

        try setSchemaVersion(5)
    }

    /// Phase 7 (playhead-4my.7.1 / 7.3-fix): Migrate `correction_events` to v6 schema.
    ///
    /// Handles three upgrade paths:
    ///   1. **No table exists** (fresh DB or v4 that never reached v5 correction_events):
    ///      CREATE TABLE with the v6 schema directly.
    ///   2. **Old-schema table exists** (0.6 shipped v5 with `correctionScope`,
    ///      `atomOrdinalRange`, `evidenceJSON` columns): Rebuild the table to gain
    ///      the new `scope` column, FK constraint, and drop dead columns. Old row
    ///      data is migrated: `correctionScope` → `scope`.
    ///   3. **v6-schema table already exists** (test DBs or re-run): No-op via
    ///      `addColumnIfNeeded` guards.
    ///
    /// The table rebuild in path (2) is necessary because SQLite cannot add FK
    /// constraints or NOT NULL columns (without DEFAULT) via ALTER TABLE.
    private func migrateCorrectionEventsV6IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 6 else { return }

        // Detect whether the old-schema table exists by checking for the
        // `correctionScope` column (present in 0.6, absent in v6 schema).
        let hasOldSchema = try columnExists(table: "correction_events", column: "correctionScope")

        if hasOldSchema {
            // Path 2: Rebuild the table from old schema to v6.
            // Copy existing rows, mapping correctionScope → scope.
            // source and podcastId did not exist in v5 — they default to NULL.
            try exec("""
                CREATE TABLE correction_events_v6 (
                    id               TEXT PRIMARY KEY,
                    analysisAssetId  TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                    scope            TEXT NOT NULL,
                    createdAt        REAL NOT NULL,
                    source           TEXT,
                    podcastId        TEXT
                )
                """)
            // Filter to rows with a valid parent (FK is enforced via PRAGMA
            // foreign_keys = ON). Orphaned rows whose analysisAssetId was already
            // deleted are silently discarded — they are unreachable anyway.
            try exec("""
                INSERT INTO correction_events_v6 (id, analysisAssetId, scope, createdAt)
                SELECT id, analysisAssetId, correctionScope, createdAt
                FROM correction_events
                WHERE analysisAssetId IN (SELECT id FROM analysis_assets)
                """)
            try exec("DROP TABLE correction_events")
            try exec("ALTER TABLE correction_events_v6 RENAME TO correction_events")
            // Drop the old index (now orphaned by the table rebuild).
            try exec("DROP INDEX IF EXISTS idx_ce_asset")
        } else {
            // Path 1 or 3: Create the table if it doesn't exist yet.
            try exec("""
                CREATE TABLE IF NOT EXISTS correction_events (
                    id               TEXT PRIMARY KEY,
                    analysisAssetId  TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                    scope            TEXT NOT NULL,
                    createdAt        REAL NOT NULL,
                    source           TEXT,
                    podcastId        TEXT
                )
                """)
        }

        try exec("CREATE INDEX IF NOT EXISTS idx_correction_events_asset ON correction_events(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_correction_events_scope ON correction_events(scope)")

        // Belt-and-suspenders: ensure source/podcastId exist even if a test DB
        // hand-built the table without them.
        try addColumnIfNeeded(table: "correction_events", column: "source", definition: "TEXT")
        try addColumnIfNeeded(table: "correction_events", column: "podcastId", definition: "TEXT")

        // ef2.3.1: CorrectionAttribution columns — nullable, backward-compatible.
        try addColumnIfNeeded(table: "correction_events", column: "correctionType", definition: "TEXT")
        try addColumnIfNeeded(table: "correction_events", column: "causalSource", definition: "TEXT")
        try addColumnIfNeeded(table: "correction_events", column: "targetRefsJSON", definition: "TEXT")

        try setSchemaVersion(6)
    }

    // MARK: - V7: Sponsor Knowledge Tables (Phase 8, playhead-4my.8.1)

    private func migrateSponsorKnowledgeV7IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 7 else { return }

        // Table 1: sponsor_knowledge_entries — lifecycle-managed sponsor entities.
        try exec("""
            CREATE TABLE IF NOT EXISTS sponsor_knowledge_entries (
                id                TEXT PRIMARY KEY,
                podcastId         TEXT NOT NULL,
                entityType        TEXT NOT NULL,
                entityValue       TEXT NOT NULL,
                normalizedValue   TEXT NOT NULL,
                state             TEXT NOT NULL DEFAULT 'candidate',
                confirmationCount INTEGER NOT NULL DEFAULT 0,
                rollbackCount     INTEGER NOT NULL DEFAULT 0,
                firstSeenAt       REAL NOT NULL,
                lastConfirmedAt   REAL,
                lastRollbackAt    REAL,
                decayedAt         REAL,
                blockedAt         REAL,
                aliases           TEXT,
                metadata          TEXT,
                UNIQUE(podcastId, entityType, normalizedValue)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_ske_podcast ON sponsor_knowledge_entries(podcastId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_ske_state ON sponsor_knowledge_entries(state)")
        try exec("CREATE INDEX IF NOT EXISTS idx_ske_podcast_state ON sponsor_knowledge_entries(podcastId, state)")

        // Table 2: knowledge_candidate_events — append-only provenance log.
        try exec("""
            CREATE TABLE IF NOT EXISTS knowledge_candidate_events (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL,
                entityType          TEXT NOT NULL,
                entityValue         TEXT NOT NULL,
                sourceAtomOrdinals  TEXT NOT NULL,
                transcriptVersion   TEXT NOT NULL,
                confidence          REAL NOT NULL,
                scanCohortJSON      TEXT,
                createdAt           REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_kce_asset ON knowledge_candidate_events(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_kce_created ON knowledge_candidate_events(createdAt)")

        try setSchemaVersion(7)
    }

    // MARK: - V8: Ad Copy Fingerprint Tables (Phase 9, playhead-4my.9.1)

    private func migrateFingerprintStoreV8IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 8 else { return }

        // Table 1: ad_copy_fingerprints — lifecycle-managed fingerprint entries.
        try exec("""
            CREATE TABLE IF NOT EXISTS ad_copy_fingerprints (
                id                TEXT PRIMARY KEY,
                podcastId         TEXT NOT NULL,
                fingerprintHash   TEXT NOT NULL,
                normalizedText    TEXT NOT NULL,
                state             TEXT NOT NULL DEFAULT 'candidate',
                confirmationCount INTEGER NOT NULL DEFAULT 0,
                rollbackCount     INTEGER NOT NULL DEFAULT 0,
                firstSeenAt       REAL NOT NULL,
                lastConfirmedAt   REAL,
                lastRollbackAt    REAL,
                decayedAt         REAL,
                blockedAt         REAL,
                metadata          TEXT,
                UNIQUE(podcastId, fingerprintHash)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_acf_podcast ON ad_copy_fingerprints(podcastId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_acf_state ON ad_copy_fingerprints(state)")
        try exec("CREATE INDEX IF NOT EXISTS idx_acf_podcast_state ON ad_copy_fingerprints(podcastId, state)")

        // Table 2: fingerprint_source_events — append-only provenance log.
        try exec("""
            CREATE TABLE IF NOT EXISTS fingerprint_source_events (
                id                TEXT PRIMARY KEY,
                analysisAssetId   TEXT NOT NULL,
                fingerprintHash   TEXT NOT NULL,
                sourceAdWindowId  TEXT NOT NULL,
                confidence        REAL NOT NULL,
                createdAt         REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_fse_asset ON fingerprint_source_events(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_fse_created ON fingerprint_source_events(createdAt)")

        try setSchemaVersion(8)
    }

    // MARK: - V9: Boundary Priors Table (ef2.3.5)

    private func migrateBoundaryPriorsV9IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 9 else { return }

        try exec("""
            CREATE TABLE IF NOT EXISTS boundary_priors (
                showId          TEXT NOT NULL,
                edgeDirection   TEXT NOT NULL,
                bracketTemplate TEXT NOT NULL DEFAULT '__none__',
                median          REAL NOT NULL,
                spread          REAL NOT NULL,
                sampleCount     INTEGER NOT NULL,
                lastUpdatedAt   REAL NOT NULL,
                PRIMARY KEY (showId, edgeDirection, bracketTemplate)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_bp_show ON boundary_priors(showId)")

        try setSchemaVersion(9)
    }

    // MARK: - V10: Music Bracket Trust Table (ef2.3.6)

    private func migrateBracketTrustV10IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 10 else { return }

        try exec("""
            CREATE TABLE IF NOT EXISTS music_bracket_trust (
                showId  TEXT PRIMARY KEY,
                alpha   REAL NOT NULL,
                beta    REAL NOT NULL
            )
            """)

        try setSchemaVersion(10)
    }

    // MARK: - V11: Source Demotions + Fingerprint Disputes (ef2.3.3)

    private func migrateSourceDemotionsV11IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 11 else { return }

        try exec("""
            CREATE TABLE IF NOT EXISTS source_demotions (
                showId            TEXT NOT NULL,
                causalSource      TEXT NOT NULL,
                currentMultiplier REAL NOT NULL DEFAULT 1.0,
                floor             REAL NOT NULL,
                updatedAt         REAL NOT NULL,
                PRIMARY KEY (showId, causalSource)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_sd_show ON source_demotions(showId)")

        try exec("""
            CREATE TABLE IF NOT EXISTS fingerprint_disputes (
                fingerprintId     TEXT NOT NULL,
                showId            TEXT NOT NULL,
                disputeCount      INTEGER NOT NULL DEFAULT 0,
                confirmationCount INTEGER NOT NULL DEFAULT 0,
                status            TEXT NOT NULL DEFAULT 'disputed',
                updatedAt         REAL NOT NULL,
                PRIMARY KEY (fingerprintId, showId)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_fd_show ON fingerprint_disputes(showId)")

        try setSchemaVersion(11)
    }

    // MARK: - V12: Implicit Feedback Events (ef2.3.4)

    private func migrateImplicitFeedbackV12IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 12 else { return }

        try exec("""
            CREATE TABLE IF NOT EXISTS implicit_feedback_events (
                id               TEXT PRIMARY KEY,
                signal           TEXT NOT NULL,
                analysisAssetId  TEXT NOT NULL,
                podcastId        TEXT,
                spanId           TEXT,
                timestamp        REAL NOT NULL,
                weight           REAL NOT NULL DEFAULT 0.3
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_ife_asset ON implicit_feedback_events(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_ife_podcast ON implicit_feedback_events(podcastId)")

        try setSchemaVersion(12)
    }

    /// playhead-narl.2: new storage for `.allEnabled` FM shadow captures.
    ///
    /// The harness (`playhead-narl.1`) reads these rows to evaluate
    /// `fmSchedulingEnabled` counterfactually — no production pipeline
    /// consumes them. `CREATE TABLE IF NOT EXISTS` is idempotent so existing
    /// installs upgrade cleanly.
    ///
    /// `fmResponse` is BLOB because the payload is opaque serialized FM
    /// output; downstream consumers decode per-row using ``fmModelVersion``
    /// as the gate. See ``ShadowFMResponse``.
    private func migrateShadowFMResponsesV13IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 13 else { return }

        try exec("""
            CREATE TABLE IF NOT EXISTS shadow_fm_responses (
                assetId        TEXT NOT NULL,
                windowStart    REAL NOT NULL,
                windowEnd      REAL NOT NULL,
                configVariant  TEXT NOT NULL,
                fmResponse     BLOB NOT NULL,
                capturedAt     REAL NOT NULL,
                capturedBy     TEXT NOT NULL,
                fmModelVersion TEXT,
                PRIMARY KEY (assetId, windowStart, windowEnd, configVariant)
            )
            """)
        // Supports Lane-B "find episodes with incomplete shadow coverage"
        // queries; the harness uses the same index for per-asset fetches.
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_shadow_fm_asset_variant
            ON shadow_fm_responses(assetId, configVariant)
            """)

        try setSchemaVersion(13)
    }

    /// playhead-gtt9.8: add `terminalReason TEXT` to `analysis_assets`.
    /// Idempotent.
    ///
    /// The column records the specific reason an asset landed in its
    /// current terminal state (e.g. `"fullCoverage"`,
    /// `"transcriptionBudgetExceeded"`, `"transcriptFailed"`,
    /// `"featureFailed"`, `"budgetCancelled"`,
    /// `"coverageBelowThreshold"`). Written by
    /// `AnalysisCoordinator.finalizeBackfill` at the same moment the
    /// session transitions into one of the richer terminals introduced
    /// by gtt9.8 (`completeFull`, `completeFeatureOnly`,
    /// `completeTranscriptPartial`, `failedTranscript`,
    /// `failedFeature`, `cancelledBudget`).
    ///
    /// Sentinel: legacy rows and any session still in a non-terminal
    /// state leave the column NULL. The readAsset decoder treats NULL
    /// as `nil`, which is the correct "no terminal reason yet" state.
    ///
    /// Placement: column is appended at the END of `analysis_assets`
    /// (SQLite's only supported `ADD COLUMN` position). After
    /// `addEpisodeDurationColumnIfNeeded` placed `episodeDurationSec`
    /// at index 12, `terminalReason` now sits at index 13. `readAsset`
    /// uses an explicit SELECT column list (not `SELECT *`) so ordering
    /// is robust across migrations.
    ///
    /// Rollback: `ADD COLUMN` is additive-only. No destructive rollback
    /// path. Existing INSERTs continue to work without supplying the
    /// column because the column is NULL-able.
    private func migrateTerminalReasonV14IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 14 else { return }

        try addColumnIfNeeded(
            table: "analysis_assets",
            column: "terminalReason",
            definition: "TEXT"
        )

        try setSchemaVersion(14)
    }

    /// playhead-i9dj: human-readable identifiers on `analysis_assets` and
    /// `podcast_profiles` so an exported `analysis.sqlite` is legible without
    /// joining to the SwiftData side. Adds:
    ///   * `analysis_assets.episodeTitle  TEXT`  — episode title from the feed
    ///     entry / SwiftData `Episode.title`.
    ///   * `podcast_profiles.title         TEXT` — show title from the feed /
    ///     SwiftData `Podcast.title`.
    ///
    /// Both columns are nullable so:
    ///   * Pre-i9dj rows decode unchanged (NULL for the new columns).
    ///   * The on-device write path can populate them lazily on first
    ///     observation (next download / play / scheduler enqueue) — no
    ///     synchronous one-shot backfill is needed because the values are
    ///     stable and recoverable from the SwiftData side at any time.
    ///   * Old exports without titles still parse: the corpus exporter emits
    ///     explicit JSON `null` when the column is NULL.
    ///
    /// Idempotent via `addColumnIfNeeded`. Both columns are appended at the
    /// END of their tables (SQLite's only supported ADD COLUMN position).
    /// `analysis_assets` reads use the explicit ``assetSelectColumns``
    /// (not `SELECT *`) so the new index doesn't disturb existing readers.
    /// `podcast_profiles` is read via `SELECT *` in `fetchProfile` — the
    /// reader has been updated to decode the new trailing column at the
    /// matching positional index.
    ///
    /// Rollback: `ADD COLUMN` is additive-only. There is no destructive
    /// rollback path. Downgrading the schema version leaves the columns in
    /// place but unread; existing INSERT paths continue to work because the
    /// columns are nullable and the upsert SQL preserves any pre-existing
    /// title via `COALESCE(excluded.title, podcast_profiles.title)` — see
    /// `upsertProfile`.
    private func migrateSelfDescribingTitlesV15IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 15 else { return }

        if try tableExists("analysis_assets") {
            try addColumnIfNeeded(
                table: "analysis_assets",
                column: "episodeTitle",
                definition: "TEXT"
            )
        }

        if try tableExists("podcast_profiles") {
            try addColumnIfNeeded(
                table: "podcast_profiles",
                column: "title",
                definition: "TEXT"
            )
        }

        try setSchemaVersion(15)
    }

    /// playhead-4my.10.1: durable training examples materialized once
    /// per backfill from the evidence + decision + correction ledger.
    ///
    /// **Why a separate table** (vs. a view over the existing ledgers):
    /// the ledger tables are cohort-scoped — `pruneOrphanedScansForCurrentCohort`
    /// wipes `evidence_events` and `semantic_scan_results` whose
    /// `scanCohortJSON` doesn't match the current cohort. Once a label has
    /// been assigned the bucket should outlive the cohort that produced
    /// it, so this table is intentionally NOT cohort-scoped and not
    /// touched by the prune sweep.
    ///
    /// **Foreign key** (cycle-2 M-B): `analysisAssetId` references
    /// `analysis_assets(id)` with `ON DELETE RESTRICT`. The bead's whole
    /// durability promise is that the materialized corpus outlives the
    /// cohort that produced it, so a cascading delete on the asset would
    /// silently wipe every prior cohort's training rows the moment the
    /// asset is removed. RESTRICT forces a deliberate purge path
    /// (export, archive, or explicit delete) before the asset itself can
    /// be deleted. The asset row is guaranteed to exist by the time the
    /// materializer runs (the backfill that produced the ledger entries
    /// created it).
    ///
    /// **Schema columns** mirror the bead spec field-for-field. The
    /// `evidenceSourcesJSON` column carries a JSON array (encoded by the
    /// store on insert) so distinct source orderings round-trip
    /// losslessly. `textSnapshot` is nullable because retention policy
    /// may elect to keep only the hash. `decisionCohortJSON` is nullable
    /// (cycle-2 M-A / L4): the materializer emits `nil` when no
    /// post-fusion decision overlapped this scan's window, which is
    /// distinct from "decision present but cohort serializer failed".
    ///
    /// **Indexes**: per-asset lookups are the only access pattern at
    /// HEAD, so a single composite index on `(analysisAssetId, createdAt)`
    /// covers the materializer's read path.
    private func migrateTrainingExamplesV16IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 16 else { return }

        // M4: FK is `ON DELETE RESTRICT` — not CASCADE. The bead's
        // cohort-survival contract is incompatible with cascading
        // deletes: future bead wires up `deleteAsset(id:)` would
        // silently wipe every prior cohort's training rows the moment an
        // asset is removed. RESTRICT forces the caller to explicitly
        // reckon with the training data first (export, archive, or
        // delete via a deliberate path) before deleting the asset.
        // `decisionCohortJSON` is nullable (L4) — emit JSON null when no
        // decision overlapped this scan; an empty string was
        // indistinguishable from a buggy serializer.
        try exec("""
            CREATE TABLE IF NOT EXISTS training_examples (
                id                    TEXT PRIMARY KEY,
                analysisAssetId       TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE RESTRICT,
                startAtomOrdinal      INTEGER NOT NULL,
                endAtomOrdinal        INTEGER NOT NULL,
                transcriptVersion     TEXT NOT NULL,
                startTime             REAL NOT NULL,
                endTime               REAL NOT NULL,
                textSnapshotHash      TEXT NOT NULL,
                textSnapshot          TEXT,
                bucket                TEXT NOT NULL,
                commercialIntent      TEXT NOT NULL,
                ownership             TEXT NOT NULL,
                evidenceSourcesJSON   TEXT NOT NULL,
                fmCertainty           REAL NOT NULL,
                classifierConfidence  REAL NOT NULL,
                userAction            TEXT,
                eligibilityGate       TEXT,
                scanCohortJSON        TEXT NOT NULL,
                decisionCohortJSON    TEXT,
                transcriptQuality     TEXT NOT NULL,
                createdAt             REAL NOT NULL
            )
            """)
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_training_examples_asset_created
            ON training_examples(analysisAssetId, createdAt ASC)
            """)
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_training_examples_bucket
            ON training_examples(bucket)
            """)

        try setSchemaVersion(16)
    }

    /// playhead-4my.10.1 (cycle-2 M-A): rebuild `training_examples` with the
    /// post-fix shape. The original v16 migration (commit `ae6b915`) shipped
    /// with `ON DELETE CASCADE` and a `NOT NULL decisionCohortJSON`. Cycle-1
    /// fixes M4 (FK → RESTRICT) and L4 (decisionCohortJSON nullable) only
    /// updated the `CREATE TABLE` body — DBs already opened under v16 retain
    /// the OLD shape forever because `migrateTrainingExamplesV16IfNeeded`
    /// early-returns once `schemaVersion >= 16`.
    ///
    /// The v17 migrator drops and recreates the table with the corrected
    /// shape. This is safe because, by the bead's contract, the cohort-scoped
    /// upstream ledgers are wiped on cohort flips and `training_examples` is
    /// only populated by the materializer post-rebuild — there is no real
    /// production data on a v16 row that survives a cohort transition. Any
    /// rows that do exist locally on a developer DB will be re-materialized
    /// on the next backfill from the still-warm cohort, so the drop is
    /// recoverable.
    ///
    /// Rollback: same as any DDL drop — there is no automatic downgrade. A
    /// user pinned to an earlier app build would see a v16 DB with the new
    /// shape and the old code's `INSERT` statement (which still expects the
    /// pre-cycle-2 column nullability) would simply fail loudly because the
    /// rebuilt schema is strictly more permissive (RESTRICT FK, nullable
    /// `decisionCohortJSON`). Forward-only by design.
    private func migrateTrainingExamplesV17IfNeeded() throws {
        guard (try schemaVersion() ?? 1) < 17 else { return }

        // Drop indexes first so SQLite doesn't keep dangling references after
        // the table goes. `IF EXISTS` so brand-new DBs (created at v16+ from
        // scratch via the immediately-preceding migrator) don't error.
        try exec("DROP INDEX IF EXISTS idx_training_examples_asset_created")
        try exec("DROP INDEX IF EXISTS idx_training_examples_bucket")
        try exec("DROP TABLE IF EXISTS training_examples")

        try exec("""
            CREATE TABLE training_examples (
                id                    TEXT PRIMARY KEY,
                analysisAssetId       TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE RESTRICT,
                startAtomOrdinal      INTEGER NOT NULL,
                endAtomOrdinal        INTEGER NOT NULL,
                transcriptVersion     TEXT NOT NULL,
                startTime             REAL NOT NULL,
                endTime               REAL NOT NULL,
                textSnapshotHash      TEXT NOT NULL,
                textSnapshot          TEXT,
                bucket                TEXT NOT NULL,
                commercialIntent      TEXT NOT NULL,
                ownership             TEXT NOT NULL,
                evidenceSourcesJSON   TEXT NOT NULL,
                fmCertainty           REAL NOT NULL,
                classifierConfidence  REAL NOT NULL,
                userAction            TEXT,
                eligibilityGate       TEXT,
                scanCohortJSON        TEXT NOT NULL,
                decisionCohortJSON    TEXT,
                transcriptQuality     TEXT NOT NULL,
                createdAt             REAL NOT NULL
            )
            """)
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_training_examples_asset_created
            ON training_examples(analysisAssetId, createdAt ASC)
            """)
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_training_examples_bucket
            ON training_examples(bucket)
            """)

        try setSchemaVersion(17)
    }

    /// Reads the current schema version from `_meta`. Returns `nil` if the row
    /// is missing (only possible on a corrupted store, since `migrate()` writes
    /// it on first run).
    func schemaVersion() throws -> Int? {
        let stmt = try prepare("SELECT value FROM _meta WHERE key = 'schema_version'")
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard let raw = optionalText(stmt, 0) else { return nil }
        return Int(raw)
    }

    deinit {
        if let db { sqlite3_close_v2(db) }
    }

    static func defaultDirectory() -> URL {
        FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Playhead", isDirectory: true)
            .appendingPathComponent("AnalysisStore", isDirectory: true)
    }

    #if DEBUG
    /// Produces an atomic snapshot of the live DB at `destinationURL` using
    /// SQLite's `VACUUM INTO`. This handles WAL/concurrent-reader correctness
    /// and emits a single standalone `.sqlite` file (no sidecar `-wal`/`-shm`).
    /// The destination file must NOT already exist — callers must remove it
    /// first. DEBUG-only export paths use this to sidestep the
    /// `FileProtectionType.complete` attribute on the primary DB so Xcode's
    /// Download Container can transfer the snapshot for offline inspection.
    func vacuumInto(destinationURL: URL) throws {
        // Escape single quotes in the path for the SQL string literal.
        let escaped = destinationURL.path.replacingOccurrences(of: "'", with: "''")
        try exec("VACUUM INTO '\(escaped)'")
    }
    #endif

    // MARK: Pragmas

    private func configurePragmas() throws {
        try exec("PRAGMA journal_mode = WAL")
        try exec("PRAGMA synchronous = NORMAL")
        try exec("PRAGMA foreign_keys = ON")
        try exec("PRAGMA busy_timeout = 3000")
    }

    // MARK: DDL

    private func createTables() throws {
        // _meta — anchor for schema version + future migration coordination.
        try exec("""
            CREATE TABLE IF NOT EXISTS _meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """)

        // analysis_assets
        // playhead-i9dj: `episodeTitle` (nullable) carries the
        // human-readable episode title pulled from the feed / SwiftData
        // `Episode.title`. Populated lazily on first observation; NULL on
        // pre-i9dj rows that have not yet been touched.
        try exec("""
            CREATE TABLE IF NOT EXISTS analysis_assets (
                id                          TEXT PRIMARY KEY,
                episodeId                   TEXT NOT NULL,
                assetFingerprint            TEXT NOT NULL,
                weakFingerprint             TEXT,
                sourceURL                   TEXT NOT NULL,
                featureCoverageEndTime      REAL,
                fastTranscriptCoverageEndTime REAL,
                confirmedAdCoverageEndTime  REAL,
                analysisState               TEXT NOT NULL DEFAULT 'new',
                analysisVersion             INTEGER NOT NULL DEFAULT 1,
                capabilitySnapshot          TEXT,
                createdAt                   REAL NOT NULL DEFAULT (strftime('%s', 'now')),
                episodeTitle                TEXT
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_assets_episode ON analysis_assets(episodeId)")

        // analysis_sessions
        // bd-3bz (Phase 4): `needsShadowRetry` + `shadowRetryPodcastId` are
        // created here for fresh databases. Existing DBs pick them up via
        // `migrateAnalysisSessionsShadowRetryV4IfNeeded`.
        // H10: column is `needsShadowRetry` (camelCase) to match the rest of
        // analysis_sessions; pre-rename DBs are repaired by
        // `renameSnakeCaseShadowRetryIfNeeded`.
        try exec("""
            CREATE TABLE IF NOT EXISTS analysis_sessions (
                id                    TEXT PRIMARY KEY,
                analysisAssetId       TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                state                 TEXT NOT NULL DEFAULT 'queued',
                startedAt             REAL NOT NULL,
                updatedAt             REAL NOT NULL,
                failureReason         TEXT,
                needsShadowRetry      INTEGER NOT NULL DEFAULT 0,
                shadowRetryPodcastId  TEXT
            )
            """)
        // bd-3bz on-device hotfix: when a pre-bd-3bz database exists at the
        // store's path, the `CREATE TABLE IF NOT EXISTS` above is a silent
        // no-op against the older table shape (no `needsShadowRetry`
        // column), and the partial index below would fail with
        // "no such column: needsShadowRetry" before the v4 migration ever
        // runs. Patch the column in defensively here so both fresh and
        // upgraded databases reach the index creation with the column
        // present.
        // H10 repair: an even-older shape may carry the snake_case
        // `needs_shadow_retry` column. Rename it in place before adding the
        // camelCase column so we don't end up with both.
        try renameSnakeCaseShadowRetryIfNeeded()
        try addColumnIfNeeded(
            table: "analysis_sessions",
            column: "needsShadowRetry",
            definition: "INTEGER NOT NULL DEFAULT 0"
        )
        try addColumnIfNeeded(
            table: "analysis_sessions",
            column: "shadowRetryPodcastId",
            definition: "TEXT"
        )
        try exec("CREATE INDEX IF NOT EXISTS idx_sessions_asset ON analysis_sessions(analysisAssetId)")
        try exec("""
            CREATE INDEX IF NOT EXISTS idx_sessions_shadow_retry
            ON analysis_sessions(id)
            WHERE needsShadowRetry = 1
            """)

        // feature_windows
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_windows (
                analysisAssetId   TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startTime         REAL NOT NULL,
                endTime           REAL NOT NULL,
                rms               REAL NOT NULL,
                spectralFlux      REAL NOT NULL,
                musicProbability  REAL NOT NULL,
                speakerChangeProxyScore REAL NOT NULL DEFAULT 0,
                musicBedChangeScore REAL NOT NULL DEFAULT 0,
                musicBedOnsetScore REAL NOT NULL DEFAULT 0,
                musicBedOffsetScore REAL NOT NULL DEFAULT 0,
                musicBedLevelRaw  TEXT NOT NULL DEFAULT 'none',
                pauseProbability  REAL NOT NULL,
                speakerClusterId  INTEGER,
                jingleHash        TEXT,
                featureVersion    INTEGER NOT NULL,
                PRIMARY KEY (analysisAssetId, startTime)
            )
            """)
        try exec("""
            CREATE TABLE IF NOT EXISTS feature_extraction_state (
                analysisAssetId TEXT PRIMARY KEY REFERENCES analysis_assets(id) ON DELETE CASCADE,
                lastWindowStartTime REAL NOT NULL,
                lastWindowEndTime REAL NOT NULL,
                lastRms REAL NOT NULL,
                lastMusicProbability REAL NOT NULL,
                lastRawSpeakerChangeProxyScore REAL NOT NULL,
                penultimateRawSpeakerChangeProxyScore REAL,
                lastMagnitudesJSON TEXT NOT NULL,
                featureVersion INTEGER NOT NULL
            )
            """)

        // transcript_chunks
        try exec("""
            CREATE TABLE IF NOT EXISTS transcript_chunks (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                segmentFingerprint  TEXT NOT NULL,
                chunkIndex          INTEGER NOT NULL,
                startTime           REAL NOT NULL,
                endTime             REAL NOT NULL,
                text                TEXT NOT NULL,
                normalizedText      TEXT NOT NULL,
                pass                TEXT NOT NULL DEFAULT 'fast',
                modelVersion        TEXT NOT NULL,
                transcriptVersion   TEXT,
                atomOrdinal         INTEGER,
                weakAnchorMetadataJSON TEXT,
                speakerId           INTEGER
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_chunks_asset ON transcript_chunks(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_chunks_time ON transcript_chunks(analysisAssetId, startTime)")

        // ad_windows
        // playhead-epfk: `catalogStoreMatchSimilarity` carries the
        // per-window top similarity returned by `AdCatalogStore.matches`
        // (cosine in `[0, 1]`); nullable because not every backfill has
        // a wired store. Appended at the END of the column list so the
        // positional `SELECT *` reader below stays correct without
        // reshuffling indices.
        try exec("""
            CREATE TABLE IF NOT EXISTS ad_windows (
                id                  TEXT PRIMARY KEY,
                analysisAssetId     TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                startTime           REAL NOT NULL,
                endTime             REAL NOT NULL,
                confidence          REAL NOT NULL,
                boundaryState       TEXT NOT NULL,
                decisionState       TEXT NOT NULL DEFAULT 'candidate',
                detectorVersion     TEXT NOT NULL,
                advertiser          TEXT,
                product             TEXT,
                adDescription       TEXT,
                evidenceText        TEXT,
                evidenceStartTime   REAL,
                metadataSource      TEXT NOT NULL DEFAULT 'none',
                metadataConfidence  REAL,
                metadataPromptVersion TEXT,
                wasSkipped          INTEGER NOT NULL DEFAULT 0,
                userDismissedBanner INTEGER NOT NULL DEFAULT 0,
                evidenceSources     TEXT,
                eligibilityGate     TEXT,
                catalogStoreMatchSimilarity REAL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_ad_asset ON ad_windows(analysisAssetId)")

        // skip_cues
        try exec("""
            CREATE TABLE IF NOT EXISTS skip_cues (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL,
                cueHash TEXT NOT NULL,
                startTime REAL NOT NULL,
                endTime REAL NOT NULL,
                confidence REAL NOT NULL,
                source TEXT NOT NULL DEFAULT 'preAnalysis',
                materializedAt REAL NOT NULL,
                wasSkipped INTEGER NOT NULL DEFAULT 0,
                userDismissed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(cueHash)
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_skip_cues_asset ON skip_cues(analysisAssetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_skip_cues_time ON skip_cues(analysisAssetId, startTime)")

        // podcast_profiles
        // playhead-i9dj: `title` (nullable) carries the human-readable show
        // title pulled from the feed / SwiftData `Podcast.title`. Populated
        // lazily on first observation; NULL on pre-i9dj rows that have not
        // yet been touched.
        try exec("""
            CREATE TABLE IF NOT EXISTS podcast_profiles (
                podcastId                   TEXT PRIMARY KEY,
                sponsorLexicon              TEXT,
                normalizedAdSlotPriors      TEXT,
                repeatedCTAFragments        TEXT,
                jingleFingerprints          TEXT,
                implicitFalsePositiveCount  INTEGER NOT NULL DEFAULT 0,
                skipTrustScore              REAL NOT NULL DEFAULT 0.5,
                observationCount            INTEGER NOT NULL DEFAULT 0,
                mode                        TEXT NOT NULL DEFAULT 'shadow',
                recentFalseSkipSignals      INTEGER NOT NULL DEFAULT 0,
                traitProfileJSON            TEXT,
                title                       TEXT
            )
            """)

        // preview_budgets
        try exec("""
            CREATE TABLE IF NOT EXISTS preview_budgets (
                canonicalEpisodeKey      TEXT PRIMARY KEY,
                consumedAnalysisSeconds  REAL NOT NULL DEFAULT 0,
                graceBreakWindow         REAL NOT NULL DEFAULT 0,
                lastUpdated              REAL NOT NULL
            )
            """)

        // analysis_jobs
        try exec("""
            CREATE TABLE IF NOT EXISTS analysis_jobs (
                jobId TEXT PRIMARY KEY,
                jobType TEXT NOT NULL,
                episodeId TEXT NOT NULL,
                podcastId TEXT,
                analysisAssetId TEXT,
                workKey TEXT NOT NULL UNIQUE,
                sourceFingerprint TEXT NOT NULL,
                downloadId TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                desiredCoverageSec REAL NOT NULL,
                featureCoverageSec REAL NOT NULL DEFAULT 0,
                transcriptCoverageSec REAL NOT NULL DEFAULT 0,
                cueCoverageSec REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'queued',
                attemptCount INTEGER NOT NULL DEFAULT 0,
                nextEligibleAt REAL,
                leaseOwner TEXT,
                leaseExpiresAt REAL,
                lastErrorCode TEXT,
                createdAt REAL NOT NULL,
                updatedAt REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_jobs_state_priority ON analysis_jobs(state, priority DESC, createdAt ASC)")
        try exec("CREATE INDEX IF NOT EXISTS idx_jobs_workkey ON analysis_jobs(workKey)")
        try exec("CREATE INDEX IF NOT EXISTS idx_jobs_episode ON analysis_jobs(episodeId)")

        // backfill_jobs
        // M8: FK CASCADE so deleting an asset cleans up its backfill rows.
        // H16/M26: decisionCohortJSON removed (dead plumbing); podcastId
        // nullable because orphan/local episodes have no podcast.
        try exec("""
            CREATE TABLE IF NOT EXISTS backfill_jobs (
                jobId TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                podcastId TEXT,
                phase TEXT NOT NULL,
                coveragePolicy TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                progressCursor TEXT,
                retryCount INTEGER NOT NULL DEFAULT 0,
                deferReason TEXT,
                status TEXT NOT NULL DEFAULT 'queued',
                scanCohortJSON TEXT,
                createdAt REAL NOT NULL
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_backfill_jobs_status_priority ON backfill_jobs(status, priority DESC, createdAt ASC)")
        try exec("CREATE INDEX IF NOT EXISTS idx_backfill_jobs_asset_phase ON backfill_jobs(analysisAssetId, phase)")

        // semantic_scan_results
        // C5: `reuseKeyHash` is a SHA-256 over the canonical concatenation of
        // (analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
        // scanPass, transcriptVersion, scanCohortJSON). UNIQUE on the hash
        // gives us bounded cache growth (one row per reuse key) without the
        // cost of indexing the long scanCohortJSON column directly. Insert
        // path uses INSERT OR REPLACE so the latest write wins.
        // Rev3-M5: `phase` is the LAST column on purpose — the column list
        // ordering is referenced by SELECT statements that read by index,
        // and keeping the new column at the bottom keeps post-merge
        // ordering predictable when sibling agents add their own fields.
        try exec("""
            CREATE TABLE IF NOT EXISTS semantic_scan_results (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                windowFirstAtomOrdinal INTEGER NOT NULL,
                windowLastAtomOrdinal INTEGER NOT NULL,
                windowStartTime REAL NOT NULL,
                windowEndTime REAL NOT NULL,
                scanPass TEXT NOT NULL,
                transcriptQuality TEXT NOT NULL,
                disposition TEXT NOT NULL,
                spansJSON TEXT NOT NULL,
                status TEXT NOT NULL,
                attemptCount INTEGER NOT NULL DEFAULT 0,
                errorContext TEXT,
                inputTokenCount INTEGER,
                outputTokenCount INTEGER,
                latencyMs REAL,
                prewarmHit INTEGER NOT NULL DEFAULT 0,
                scanCohortJSON TEXT NOT NULL,
                transcriptVersion TEXT NOT NULL,
                reuseKeyHash TEXT NOT NULL,
                runMode TEXT NOT NULL DEFAULT 'shadow',
                jobPhase TEXT NOT NULL DEFAULT 'shadow',
                UNIQUE(reuseKeyHash)
            )
            """)
        // Cycle 8 reconciliation:
        //   * `runMode` (C4 Rev3-M5) — shadow vs targeted run-mode discriminator.
        //     Renamed from `phase` → `runMode` to disambiguate from B6's jobPhase.
        //   * `jobPhase` (B6 Rev3-M6) — BackfillJobPhase.rawValue, the originating
        //     backfill job phase (harvester/lexical/audit/fullEpisodeScan).
        // Both columns are defensively added here via `addColumnIfNeeded` so
        // pre-existing DBs pick them up without a schema-version bump.
        try addColumnIfNeeded(
            table: "semantic_scan_results",
            column: "runMode",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try addColumnIfNeeded(
            table: "semantic_scan_results",
            column: "jobPhase",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try exec("CREATE INDEX IF NOT EXISTS idx_semantic_scan_results_asset_pass ON semantic_scan_results(analysisAssetId, scanPass)")
        try exec("CREATE INDEX IF NOT EXISTS idx_semantic_scan_results_asset_runMode ON semantic_scan_results(analysisAssetId, runMode)")
        try exec("CREATE INDEX IF NOT EXISTS idx_semantic_scan_results_asset_jobPhase ON semantic_scan_results(analysisAssetId, jobPhase)")
        // M1/L3: dropped `idx_semantic_scan_results_reuse` and
        // `idx_semantic_scan_results_reuse_cohort` — neither is used by the
        // primary reuse query (which now hits the UNIQUE(reuseKeyHash) index).
        // The asset_pass index above is sufficient for diagnostic listings.

        // evidence_events
        // playhead-fn0: UNIQUE on (asset, eventType, sourceType, atomOrdinals,
        // evidenceJSON, cohort, transcriptVersion). This preserves distinct FM
        // refinement spans that cover the same atom range but differ
        // materially in payload while also keeping append-only audit across
        // transcript revisions. Exact reruns of the same transcript version
        // remain idempotent.
        // Inserts use INSERT OR IGNORE for silent idempotent dedup.
        //
        // H-2: evidence events are intentionally NOT FK-linked to
        // `semantic_scan_results`. They reference the asset directly via
        // `analysisAssetId` so that when an older scan row is replaced via
        // `reuseKeyHash` collision (INSERT OR REPLACE), the historical
        // evidence rows remain for audit purposes. Idempotency of re-runs
        // is handled by the UNIQUE(asset, eventType, sourceType,
        // atomOrdinals, evidenceJSON, scanCohortJSON, transcriptVersion)
        // constraint plus INSERT OR IGNORE: an exact rerun silently dedups,
        // while a new transcriptVersion, cohort, or materially different FM
        // span naturally appends.
        // Rev3-M5: `phase` is the LAST column on purpose, mirroring
        // `semantic_scan_results`. NOT included in the UNIQUE constraint:
        // the same logical span (asset, eventType, sourceType, atoms,
        // body, cohort, transcriptVersion) is the natural identity, and
        // the phase tag is an attribute of the row, not part of its key.
        try exec("""
            CREATE TABLE IF NOT EXISTS evidence_events (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                eventType TEXT NOT NULL,
                sourceType TEXT NOT NULL,
                atomOrdinals TEXT NOT NULL,
                evidenceJSON TEXT NOT NULL,
                scanCohortJSON TEXT NOT NULL,
                transcriptVersion TEXT NOT NULL DEFAULT '',
                createdAt REAL NOT NULL,
                runMode TEXT NOT NULL DEFAULT 'shadow',
                jobPhase TEXT NOT NULL DEFAULT 'shadow',
                UNIQUE(
                    analysisAssetId, eventType, sourceType, atomOrdinals,
                    evidenceJSON, scanCohortJSON, transcriptVersion
                )
            )
            """)
        // Cycle 8 reconciliation: defensively add both `runMode` (C4) and
        // `jobPhase` (B6) after the V2/V3 rebuilds that would have stripped
        // them. See semantic_scan_results above for the naming rationale.
        try addColumnIfNeeded(
            table: "evidence_events",
            column: "runMode",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try addColumnIfNeeded(
            table: "evidence_events",
            column: "jobPhase",
            definition: "TEXT NOT NULL DEFAULT 'shadow'"
        )
        try exec("CREATE INDEX IF NOT EXISTS idx_evidence_events_asset_created ON evidence_events(analysisAssetId, createdAt ASC)")

        // bd-m8k: podcast_planner_state — per-podcast CoveragePlanner state
        // (observed episode count, episodes since last full rescan, recall
        // ring, cached stable-recall flag). Sibling table to
        // `podcast_profiles`; NOT backfilled on migration. Rows are created
        // lazily on first access. The recall ring stores the most recent
        // `plannerRecallRingSize` (3) full-rescan recall samples; the
        // flag is recomputed on every state mutation. Cycle 4 B4: two new
        // columns — `episodesObservedWithoutSampleCount` and
        // `narrowingAllPhasesEmptyEpisodeCount` — persist per-podcast
        // signals that previously lived only on the runner actor and were
        // therefore reset per `runPendingBackfill` call.
        try exec("""
            CREATE TABLE IF NOT EXISTS podcast_planner_state (
                podcastId                                 TEXT PRIMARY KEY,
                observedEpisodeCount                      INTEGER NOT NULL DEFAULT 0,
                episodesSinceLastFullRescan               INTEGER NOT NULL DEFAULT 0,
                stablePrecisionFlag                       INTEGER NOT NULL DEFAULT 0,
                lastFullRescanAt                          REAL,
                precisionSample1                          REAL,
                precisionSample2                          REAL,
                precisionSample3                          REAL,
                precisionSampleCount                      INTEGER NOT NULL DEFAULT 0,
                episodesObservedWithoutSampleCount        INTEGER NOT NULL DEFAULT 0,
                narrowingAllPhasesEmptyEpisodeCount       INTEGER NOT NULL DEFAULT 0
            )
            """)

        // FTS5 virtual table over transcript_chunks
        try exec("""
            CREATE VIRTUAL TABLE IF NOT EXISTS transcript_chunks_fts USING fts5(
                text,
                normalizedText,
                content='transcript_chunks',
                content_rowid='rowid'
            )
            """)

        // Content-sync triggers
        try exec("""
            CREATE TRIGGER IF NOT EXISTS transcript_chunks_ai AFTER INSERT ON transcript_chunks BEGIN
                INSERT INTO transcript_chunks_fts(rowid, text, normalizedText)
                VALUES (new.rowid, new.text, new.normalizedText);
            END
            """)
        try exec("""
            CREATE TRIGGER IF NOT EXISTS transcript_chunks_ad AFTER DELETE ON transcript_chunks BEGIN
                INSERT INTO transcript_chunks_fts(transcript_chunks_fts, rowid, text, normalizedText)
                VALUES ('delete', old.rowid, old.text, old.normalizedText);
            END
            """)
        try exec("""
            CREATE TRIGGER IF NOT EXISTS transcript_chunks_au AFTER UPDATE ON transcript_chunks BEGIN
                INSERT INTO transcript_chunks_fts(transcript_chunks_fts, rowid, text, normalizedText)
                VALUES ('delete', old.rowid, old.text, old.normalizedText);
                INSERT INTO transcript_chunks_fts(rowid, text, normalizedText)
                VALUES (new.rowid, new.text, new.normalizedText);
            END
            """)

        // decoded_spans (Phase 5, playhead-4my.5.2)
        // New table — additive-only migration. Never extends ad_windows.
        // `anchorProvenanceJSON` is a JSON-encoded [AnchorRef] array.
        // INSERT OR REPLACE makes re-runs idempotent (same id → same row).
        try exec("""
            CREATE TABLE IF NOT EXISTS decoded_spans (
                id                  TEXT PRIMARY KEY,
                assetId             TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                firstAtomOrdinal    INTEGER NOT NULL,
                lastAtomOrdinal     INTEGER NOT NULL,
                startTime           REAL NOT NULL,
                endTime             REAL NOT NULL,
                anchorProvenanceJSON TEXT NOT NULL DEFAULT '[]'
            )
            """)
        try exec("CREATE INDEX IF NOT EXISTS idx_decoded_spans_asset ON decoded_spans(assetId)")
        try exec("CREATE INDEX IF NOT EXISTS idx_decoded_spans_asset_time ON decoded_spans(assetId, startTime)")
    }

    // MARK: - CRUD: analysis_assets

    func insertAsset(_ asset: AnalysisAsset) throws {
        // playhead-h7r: new artifacts get an explicit `artifact_class` at
        // insert time. Legacy rows predating the column use the
        // `DEFAULT 'media'` sentinel from the migration.
        // playhead-gtt9.1.1: `episodeDurationSec` is bound at insert
        // time only if the caller pre-populated it; spool-time writes
        // go through ``updateEpisodeDuration`` once the shard sum is
        // known. Placeholder inserts from `resolveAssetId` pass nil
        // and the column lands NULL.
        // playhead-i9dj: `episodeTitle` is bound at insert time when the
        // caller passed it on `AnalysisAsset.episodeTitle`. Existing
        // call-sites that don't yet thread the title leave it `nil`, and
        // ``updateAssetEpisodeTitle(id:episodeTitle:)`` populates it
        // lazily on first observation.
        let sql = """
            INSERT INTO analysis_assets
            (id, episodeId, assetFingerprint, weakFingerprint, sourceURL,
             featureCoverageEndTime, fastTranscriptCoverageEndTime, confirmedAdCoverageEndTime,
             analysisState, analysisVersion, capabilitySnapshot, artifact_class,
             episodeDurationSec, episodeTitle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, asset.id)
        bind(stmt, 2, asset.episodeId)
        bind(stmt, 3, asset.assetFingerprint)
        bind(stmt, 4, asset.weakFingerprint)
        bind(stmt, 5, asset.sourceURL)
        bind(stmt, 6, asset.featureCoverageEndTime)
        bind(stmt, 7, asset.fastTranscriptCoverageEndTime)
        bind(stmt, 8, asset.confirmedAdCoverageEndTime)
        bind(stmt, 9, asset.analysisState)
        bind(stmt, 10, asset.analysisVersion)
        bind(stmt, 11, asset.capabilitySnapshot)
        bind(stmt, 12, asset.artifactClass.rawValue)
        bind(stmt, 13, asset.episodeDurationSec)
        bind(stmt, 14, asset.episodeTitle)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// playhead-h7r: count `analysis_assets` rows grouped by
    /// `artifact_class`. Used by ``StorageBudget`` as a lightweight way
    /// to observe per-class cardinality; byte accounting is the job of
    /// the injected size accessor, not this store.
    func countAssetsByArtifactClass() throws -> [ArtifactClass: Int] {
        let sql = "SELECT artifact_class, COUNT(*) FROM analysis_assets GROUP BY artifact_class"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var out: [ArtifactClass: Int] = [:]
        while sqlite3_step(stmt) == SQLITE_ROW {
            let raw = optionalText(stmt, 0)
            let cls = ArtifactClass.fromPersistedRaw(raw)
            let count = Int(sqlite3_column_int(stmt, 1))
            out[cls, default: 0] += count
        }
        return out
    }

    func fetchAsset(id: String) throws -> AnalysisAsset? {
        let sql = """
            SELECT \(assetSelectColumns)
            FROM analysis_assets
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readAsset(stmt)
    }

    /// Production-safe paginated read of `analysis_assets`. Returns rows
    /// ordered by `createdAt DESC, rowid DESC` (matching `fetchAllAssets`),
    /// starting at `offset`, capped at `limit`. Callers iterate until a
    /// short page returns to drain the whole table without ever holding
    /// more than `limit` rows in memory at once.
    ///
    /// `limit` and `offset` are both clamped to `>= 0`; a `limit` of `0`
    /// returns an empty array.
    func fetchAssets(limit: Int, offset: Int) throws -> [AnalysisAsset] {
        let safeLimit = max(0, limit)
        let safeOffset = max(0, offset)
        guard safeLimit > 0 else { return [] }
        let sql = """
            SELECT \(assetSelectColumns)
            FROM analysis_assets
            ORDER BY createdAt DESC, rowid DESC
            LIMIT ? OFFSET ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_int64(stmt, 1, Int64(safeLimit))
        sqlite3_bind_int64(stmt, 2, Int64(safeOffset))
        var results: [AnalysisAsset] = []
        results.reserveCapacity(safeLimit)
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readAsset(stmt))
        }
        return results
    }

#if DEBUG
    /// Fetch every analysis asset in the store, ordered by creation time (newest first).
    ///
    /// DEBUG-ONLY: this method loads every `analysis_assets` row into memory in
    /// a single pass with no pagination, no `LIMIT`, and no streaming. It exists
    /// solely to back `DebugEpisodeExporter.buildLibraryExport`, which is itself
    /// `#if DEBUG`-gated. It is not safe for production callers — on a real
    /// listener's library this can OOM or stall the actor for seconds.
    ///
    /// Production callers should use `fetchAssets(limit:offset:)` and iterate.
    func fetchAllAssets() throws -> [AnalysisAsset] {
        let sql = """
            SELECT \(assetSelectColumns)
            FROM analysis_assets
            ORDER BY createdAt DESC, rowid DESC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var results: [AnalysisAsset] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readAsset(stmt))
        }
        return results
    }

    /// DEBUG-only: pin the `createdAt` of an existing `analysis_assets`
    /// row to an explicit value. Used by ordering tests that need
    /// deterministic tie-break semantics without relying on wall-clock
    /// behavior of `strftime('%s','now')` at insert time. The production
    /// insert path populates `createdAt` via the schema default; this
    /// setter exists solely so narl.2 tests can assert "two assets with
    /// identical `createdAt` tie-break by id ASC" without flaking when
    /// the clock rolls a second mid-insert.
    func setAssetCreatedAtForTesting(id: String, createdAt: Double) throws {
        let sql = "UPDATE analysis_assets SET createdAt = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_double(stmt, 1, createdAt)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Debug-only: look up the `podcastId` recorded for an episode in the
    /// `analysis_jobs` table. Returns `nil` when no job row exists or when
    /// the stored `podcastId` is NULL. The `analysis_jobs` row is the only
    /// table that reliably carries podcastId for a given episodeId (the
    /// `analysis_assets` table was never given that column), so the export
    /// path queries it for show-level grouping downstream (playhead-narl.1
    /// HIGH-3). Newest row wins if multiple jobs exist for the same episode
    /// — in practice distinct podcastIds for the same episode would be a
    /// data anomaly, not a real case.
    ///
    /// Debug-only: like `fetchAllAssets`, this is only intended for corpus
    /// export and the test harness. Production callers should not depend
    /// on it.
    func fetchPodcastId(forEpisodeId episodeId: String) throws -> String? {
        let sql = """
            SELECT podcastId
            FROM analysis_jobs
            WHERE episodeId = ? AND podcastId IS NOT NULL
            ORDER BY createdAt DESC, rowid DESC
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return optionalText(stmt, 0)
    }
#endif

    func updateAssetState(id: String, state: String) throws {
        let sql = "UPDATE analysis_assets SET analysisState = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// playhead-gtt9.8: atomically update `analysisState` and
    /// `terminalReason` for an asset. Callers use this exclusively at
    /// terminal transitions; non-terminal transitions (queued →
    /// spooling, etc.) continue to go through ``updateAssetState``.
    ///
    /// - Parameters:
    ///   - id: asset primary key.
    ///   - state: the new `analysisState` rawValue.
    ///   - terminalReason: a short machine-readable reason string (see
    ///     ``AnalysisAsset/terminalReason`` for canonical values). Pass
    ///     `nil` explicitly to clear a stale reason (rare — happens only
    ///     on the recovery-sweep path that resets a stranded session
    ///     back to `.queued`).
    func updateAssetState(
        id: String,
        state: String,
        terminalReason: String?
    ) throws {
        let sql = """
            UPDATE analysis_assets
            SET analysisState = ?, terminalReason = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        bind(stmt, 2, terminalReason)
        bind(stmt, 3, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchAssetByEpisodeId(_ episodeId: String) throws -> AnalysisAsset? {
        let sql = """
            SELECT \(assetSelectColumns)
            FROM analysis_assets
            WHERE episodeId = ?
            ORDER BY createdAt DESC, rowid DESC
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readAsset(stmt)
    }

    /// playhead-hkn1: latest-per-episode map of every asset in the
    /// store. Single SQL pass, no `IN`-clause variable count to worry
    /// about. The provider uses this to filter its SwiftData
    /// descriptor at the predicate level — only Episodes that already
    /// have an `analysis_assets` row get materialized.
    ///
    /// "Latest" follows the same precedence as
    /// `fetchAssetByEpisodeId`: `ORDER BY createdAt DESC, rowid DESC`,
    /// keep the first row encountered per episodeId.
    func fetchLatestAssetByEpisodeIdMap() throws -> [String: AnalysisAsset] {
        let sql = """
            SELECT \(assetSelectColumns)
            FROM analysis_assets
            ORDER BY createdAt DESC, rowid DESC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var results: [String: AnalysisAsset] = [:]
        while sqlite3_step(stmt) == SQLITE_ROW {
            let asset = readAsset(stmt)
            if results[asset.episodeId] == nil {
                results[asset.episodeId] = asset
            }
        }
        return results
    }

    /// playhead-hkn1: bulk variant of ``fetchAssetByEpisodeId`` for hot
    /// paths that would otherwise issue N actor-hops in a row (the
    /// Activity-screen snapshot provider being the canonical case —
    /// pre-hkn1 it serialized one round-trip per episode on the main
    /// actor and froze the UI on Dan's dogfood device).
    ///
    /// Returns a `[episodeId: AnalysisAsset]` dictionary matching the
    /// "latest asset per episode" semantic of the single-fetch sibling
    /// (`ORDER BY createdAt DESC` then keep the first row encountered
    /// per episodeId). Episodes with no row in `analysis_assets` are
    /// simply absent from the dictionary; callers should treat absence
    /// the same way the single-fetch path treats `nil`.
    ///
    /// Empty input returns an empty dictionary without touching SQLite.
    /// Larger inputs are chunked at 500 placeholders per statement so
    /// we stay well under SQLite's `SQLITE_MAX_VARIABLE_NUMBER` default
    /// (999 historically; 32766 on recent builds — both are safe).
    /// All bindings go through the prepared-statement bind helpers; we
    /// never interpolate user values into the SQL string.
    func fetchAssetsByEpisodeIds(_ episodeIds: Set<String>) throws -> [String: AnalysisAsset] {
        guard !episodeIds.isEmpty else { return [:] }
        let chunkSize = 500
        var results: [String: AnalysisAsset] = [:]
        results.reserveCapacity(episodeIds.count)

        // Stable-order chunking so test-time iteration is deterministic
        // and the bind indices below align with the chunk slice.
        let allIds = Array(episodeIds)
        var index = 0
        while index < allIds.count {
            let end = min(index + chunkSize, allIds.count)
            let slice = Array(allIds[index..<end])
            let placeholders = slice.map { _ in "?" }.joined(separator: ", ")
            let sql = """
                SELECT \(assetSelectColumns)
                FROM analysis_assets
                WHERE episodeId IN (\(placeholders))
                ORDER BY createdAt DESC, rowid DESC
                """
            let stmt = try prepare(sql)
            defer { sqlite3_finalize(stmt) }
            for (i, id) in slice.enumerated() {
                bind(stmt, Int32(i + 1), id)
            }
            while sqlite3_step(stmt) == SQLITE_ROW {
                let asset = readAsset(stmt)
                // First row wins per episodeId — matches the single-
                // fetch's `LIMIT 1 ORDER BY createdAt DESC, rowid DESC`.
                if results[asset.episodeId] == nil {
                    results[asset.episodeId] = asset
                }
            }
            index = end
        }

        return results
    }

    func deleteAsset(id: String) throws {
        let sql = "DELETE FROM analysis_assets WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// playhead-gtt9.1.1: explicit column list for every `SELECT` that
    /// feeds ``readAsset``. We moved off `SELECT *` when
    /// `episodeDurationSec` was appended because `SELECT *` returns
    /// columns in CREATE TABLE order followed by ADD COLUMN order, so
    /// index positions are fragile across future migrations. The order
    /// here is the contract ``readAsset`` decodes against.
    ///
    /// playhead-gtt9.8: `terminalReason` appended at index 13.
    /// playhead-i9dj: `episodeTitle` appended at index 14.
    private static let assetSelectColumns: String = """
        id, episodeId, assetFingerprint, weakFingerprint, sourceURL, \
        featureCoverageEndTime, fastTranscriptCoverageEndTime, \
        confirmedAdCoverageEndTime, analysisState, analysisVersion, \
        capabilitySnapshot, artifact_class, episodeDurationSec, \
        terminalReason, episodeTitle
        """

    private var assetSelectColumns: String { Self.assetSelectColumns }

    private func readAsset(_ stmt: OpaquePointer?) -> AnalysisAsset {
        // playhead-gtt9.1.1: decoded against the explicit
        // ``assetSelectColumns`` ordering, not `SELECT *`. Indices
        // correspond 1:1 with the columns listed there.
        //   0: id
        //   1: episodeId
        //   2: assetFingerprint
        //   3: weakFingerprint
        //   4: sourceURL
        //   5: featureCoverageEndTime
        //   6: fastTranscriptCoverageEndTime
        //   7: confirmedAdCoverageEndTime
        //   8: analysisState
        //   9: analysisVersion
        //  10: capabilitySnapshot
        //  11: artifact_class  (playhead-h7r)
        //  12: episodeDurationSec  (playhead-gtt9.1.1)
        //  13: terminalReason  (playhead-gtt9.8)
        //  14: episodeTitle  (playhead-i9dj)
        AnalysisAsset(
            id: text(stmt, 0),
            episodeId: text(stmt, 1),
            assetFingerprint: text(stmt, 2),
            weakFingerprint: optionalText(stmt, 3),
            sourceURL: text(stmt, 4),
            featureCoverageEndTime: optionalDouble(stmt, 5),
            fastTranscriptCoverageEndTime: optionalDouble(stmt, 6),
            confirmedAdCoverageEndTime: optionalDouble(stmt, 7),
            analysisState: text(stmt, 8),
            analysisVersion: Int(sqlite3_column_int(stmt, 9)),
            capabilitySnapshot: optionalText(stmt, 10),
            artifactClass: ArtifactClass.fromPersistedRaw(optionalText(stmt, 11)),
            episodeDurationSec: optionalDouble(stmt, 12),
            terminalReason: optionalText(stmt, 13),
            episodeTitle: optionalText(stmt, 14)
        )
    }

    private func readFeatureExtractionCheckpoint(_ stmt: OpaquePointer?) throws -> FeatureExtractionCheckpoint {
        FeatureExtractionCheckpoint(
            analysisAssetId: text(stmt, 0),
            lastWindowStartTime: sqlite3_column_double(stmt, 1),
            lastWindowEndTime: sqlite3_column_double(stmt, 2),
            lastRms: sqlite3_column_double(stmt, 3),
            lastMusicProbability: sqlite3_column_double(stmt, 4),
            lastRawSpeakerChangeProxyScore: sqlite3_column_double(stmt, 5),
            penultimateRawSpeakerChangeProxyScore: optionalDouble(stmt, 6),
            lastMagnitudes: try decodeMagnitudesJSON(text(stmt, 7)),
            featureVersion: Int(sqlite3_column_int(stmt, 8))
        )
    }

    // MARK: - CRUD: analysis_sessions

    func insertSession(_ session: AnalysisSession) throws {
        let sql = """
            INSERT INTO analysis_sessions
                (id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                 needsShadowRetry, shadowRetryPodcastId)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, session.id)
        bind(stmt, 2, session.analysisAssetId)
        bind(stmt, 3, session.state)
        bind(stmt, 4, session.startedAt)
        bind(stmt, 5, session.updatedAt)
        bind(stmt, 6, session.failureReason)
        bind(stmt, 7, session.needsShadowRetry ? 1 : 0)
        bind(stmt, 8, session.shadowRetryPodcastId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchSession(id: String) throws -> AnalysisSession? {
        let sql = """
            SELECT id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                   needsShadowRetry, shadowRetryPodcastId
            FROM analysis_sessions WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readSession(stmt)
    }

    func fetchLatestSessionForAsset(assetId: String) throws -> AnalysisSession? {
        let sql = """
            SELECT id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                   needsShadowRetry, shadowRetryPodcastId
            FROM analysis_sessions WHERE analysisAssetId = ?
            ORDER BY updatedAt DESC LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readSession(stmt)
    }

    private func readSession(_ stmt: OpaquePointer?) -> AnalysisSession {
        AnalysisSession(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            state: text(stmt, 2),
            startedAt: sqlite3_column_double(stmt, 3),
            updatedAt: sqlite3_column_double(stmt, 4),
            failureReason: optionalText(stmt, 5),
            needsShadowRetry: sqlite3_column_int(stmt, 6) != 0,
            shadowRetryPodcastId: optionalText(stmt, 7)
        )
    }

    func updateSessionState(id: String, state: String, failureReason: String? = nil) throws {
        let sql = "UPDATE analysis_sessions SET state = ?, updatedAt = ?, failureReason = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, failureReason)
        bind(stmt, 4, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// bd-3bz (Phase 4): mark a session as needing a Foundation Models shadow
    /// retry and record the podcastId captured at bail time. Called by
    /// `AdDetectionService.runShadowFMPhase` when the `canUseFoundationModels`
    /// guard short-circuits. The `PlayheadRuntime` capability observer drains
    /// flagged sessions after a 60s-stable-true debounce.
    func markSessionNeedsShadowRetry(id: String, podcastId: String) throws {
        let sql = """
            UPDATE analysis_sessions
            SET needsShadowRetry = 1,
                shadowRetryPodcastId = ?,
                updatedAt = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// bd-3bz (Phase 4): clear the shadow-retry flag after a successful
    /// `retryShadowFMPhaseForSession` drain.
    func clearSessionShadowRetry(id: String) throws {
        let sql = """
            UPDATE analysis_sessions
            SET needsShadowRetry = 0,
                shadowRetryPodcastId = NULL,
                updatedAt = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// bd-3bz (Phase 4): fetch all sessions currently flagged for a shadow
    /// retry. Order is stable (by updatedAt ASC) so drains are deterministic
    /// in tests.
    func fetchSessionsNeedingShadowRetry() throws -> [AnalysisSession] {
        let sql = """
            SELECT id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                   needsShadowRetry, shadowRetryPodcastId
            FROM analysis_sessions
            WHERE needsShadowRetry = 1
            ORDER BY updatedAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var results: [AnalysisSession] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readSession(stmt))
        }
        return results
    }

    /// Fetch every session currently in `.failed` whose `failureReason`
    /// begins with the supplied prefix. Used by the coverage-guard recovery
    /// sweep to discover sessions that were aborted because the transcript
    /// had not yet caught up to the minimum coverage ratio. Order is stable
    /// (by `updatedAt ASC`) so sweeps are deterministic in tests.
    func fetchFailedSessions(withFailureReasonPrefix prefix: String) throws -> [AnalysisSession] {
        let sql = """
            SELECT id, analysisAssetId, state, startedAt, updatedAt, failureReason,
                   needsShadowRetry, shadowRetryPodcastId
            FROM analysis_sessions
            WHERE state = ? AND failureReason LIKE ?
            ORDER BY updatedAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, SessionState.failed.rawValue)
        // SQLite LIKE is case-insensitive on ASCII by default; we escape no
        // metacharacters because the supplied prefix is always a literal
        // string from production code (no user input path).
        bind(stmt, 2, "\(prefix)%")
        var results: [AnalysisSession] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readSession(stmt))
        }
        return results
    }

    func updateFeatureCoverage(id: String, endTime: Double) throws {
        let sql = "UPDATE analysis_assets SET featureCoverageEndTime = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, endTime)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// playhead-gtt9.1.1: persist the total audio duration on the
    /// `analysis_assets` row so that coverage-guard denominators
    /// survive resume-from-persisted-`.backfill` paths where
    /// `activeShards` is never rehydrated.
    ///
    /// The value is the sum of shard durations computed during spool
    /// (see ``AnalysisCoordinator/runFromSpooling``). Idempotent —
    /// repeated calls overwrite. Callers should only write positive
    /// values; negative or zero durations are accepted verbatim but
    /// treated as "missing" by
    /// ``AnalysisCoordinator/resolveEpisodeDuration(activeShards:persistedDuration:)``.
    func updateEpisodeDuration(id: String, episodeDurationSec: Double) throws {
        let sql = "UPDATE analysis_assets SET episodeDurationSec = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeDurationSec)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// playhead-i9dj: persist the human-readable episode title on the
    /// `analysis_assets` row so an exported `analysis.sqlite` is legible
    /// without joining to the SwiftData side.
    ///
    /// Idempotent, lazy backfill semantics: passing a non-nil title
    /// always overwrites the column; passing `nil` is a no-op (keeps any
    /// previously-recorded title). Callers fetch the canonical title
    /// from SwiftData (`Episode.title`) on first observation — typically
    /// at download enqueue time or playback-start — and invoke this
    /// setter; later writes from the same call site re-confirm the
    /// (usually unchanged) title at zero cost.
    ///
    /// - Parameters:
    ///   - id: `analysis_assets.id` of the row to update.
    ///   - episodeTitle: the title to persist, or `nil` to leave the
    ///     existing value untouched. Whitespace-only strings are written
    ///     verbatim — the caller is responsible for trimming if desired.
    func updateAssetEpisodeTitle(id: String, episodeTitle: String?) throws {
        // nil-write is a no-op rather than a NULL-overwrite. This
        // matches the bead's "lazy backfill" intent: a reconciler that
        // doesn't yet have the title in scope must not erase one that a
        // prior, better-informed call site already wrote.
        guard let episodeTitle else { return }

        let sql = "UPDATE analysis_assets SET episodeTitle = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeTitle)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    // MARK: - CRUD: feature_windows

    func insertFeatureWindows(_ windows: [FeatureWindow]) throws {
        guard !windows.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for fw in windows {
                try insertFeatureWindow(fw)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func insertFeatureWindow(_ fw: FeatureWindow) throws {
        let sql = """
            INSERT OR REPLACE INTO feature_windows
            (analysisAssetId, startTime, endTime, rms, spectralFlux,
             musicProbability, speakerChangeProxyScore, musicBedChangeScore,
             musicBedOnsetScore, musicBedOffsetScore, musicBedLevelRaw,
             pauseProbability, speakerClusterId, jingleHash, featureVersion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, fw.analysisAssetId)
        bind(stmt, 2, fw.startTime)
        bind(stmt, 3, fw.endTime)
        bind(stmt, 4, fw.rms)
        bind(stmt, 5, fw.spectralFlux)
        bind(stmt, 6, fw.musicProbability)
        bind(stmt, 7, fw.speakerChangeProxyScore)
        bind(stmt, 8, fw.musicBedChangeScore)
        bind(stmt, 9, fw.musicBedOnsetScore)
        bind(stmt, 10, fw.musicBedOffsetScore)
        bind(stmt, 11, fw.musicBedLevel.rawValue)
        bind(stmt, 12, fw.pauseProbability)
        bind(stmt, 13, fw.speakerClusterId)
        bind(stmt, 14, fw.jingleHash)
        bind(stmt, 15, fw.featureVersion)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateFeatureWindowSpeakerChangeProxyScore(
        assetId: String,
        startTime: Double,
        endTime: Double,
        featureVersion: Int,
        speakerChangeProxyScore: Double
    ) throws {
        let sql = """
            UPDATE feature_windows
            SET speakerChangeProxyScore = ?
            WHERE analysisAssetId = ? AND startTime = ? AND endTime = ? AND featureVersion = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, speakerChangeProxyScore)
        bind(stmt, 2, assetId)
        bind(stmt, 3, startTime)
        bind(stmt, 4, endTime)
        bind(stmt, 5, featureVersion)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func persistFeatureExtractionBatch(
        assetId: String,
        windows: [FeatureWindow],
        priorWindowUpdate: FeatureWindowSpeakerChangeProxyUpdate?,
        checkpoint: FeatureExtractionCheckpoint?,
        coverageEndTime: Double?
    ) throws {
        guard priorWindowUpdate != nil || !windows.isEmpty || checkpoint != nil || coverageEndTime != nil else {
            return
        }

        try exec("BEGIN TRANSACTION")
        do {
            if let priorWindowUpdate {
                try updateFeatureWindowSpeakerChangeProxyScore(
                    assetId: priorWindowUpdate.assetId,
                    startTime: priorWindowUpdate.startTime,
                    endTime: priorWindowUpdate.endTime,
                    featureVersion: priorWindowUpdate.featureVersion,
                    speakerChangeProxyScore: priorWindowUpdate.speakerChangeProxyScore
                )
            }

            for window in windows {
                try insertFeatureWindow(window)
            }

            if let checkpoint {
                try upsertFeatureExtractionCheckpoint(checkpoint)
            }

            if let coverageEndTime {
                try updateFeatureCoverage(id: assetId, endTime: coverageEndTime)
                #if DEBUG
                try triggerFeatureBatchPersistenceFaultIfNeeded(.afterCoverageUpdateBeforeCommit)
                #endif
            }

            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func upsertFeatureExtractionCheckpoint(_ checkpoint: FeatureExtractionCheckpoint) throws {
        let sql = """
            INSERT INTO feature_extraction_state
            (analysisAssetId, lastWindowStartTime, lastWindowEndTime, lastRms,
             lastMusicProbability, lastRawSpeakerChangeProxyScore,
             penultimateRawSpeakerChangeProxyScore, lastMagnitudesJSON, featureVersion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(analysisAssetId) DO UPDATE SET
                lastWindowStartTime = excluded.lastWindowStartTime,
                lastWindowEndTime = excluded.lastWindowEndTime,
                lastRms = excluded.lastRms,
                lastMusicProbability = excluded.lastMusicProbability,
                lastRawSpeakerChangeProxyScore = excluded.lastRawSpeakerChangeProxyScore,
                penultimateRawSpeakerChangeProxyScore = excluded.penultimateRawSpeakerChangeProxyScore,
                lastMagnitudesJSON = excluded.lastMagnitudesJSON,
                featureVersion = excluded.featureVersion
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, checkpoint.analysisAssetId)
        bind(stmt, 2, checkpoint.lastWindowStartTime)
        bind(stmt, 3, checkpoint.lastWindowEndTime)
        bind(stmt, 4, checkpoint.lastRms)
        bind(stmt, 5, checkpoint.lastMusicProbability)
        bind(stmt, 6, checkpoint.lastRawSpeakerChangeProxyScore)
        bind(stmt, 7, checkpoint.penultimateRawSpeakerChangeProxyScore)
        bind(stmt, 8, try encodeMagnitudesJSON(checkpoint.lastMagnitudes))
        bind(stmt, 9, checkpoint.featureVersion)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchFeatureExtractionCheckpoint(
        assetId: String,
        featureVersion: Int,
        endingAt endTime: Double
    ) throws -> FeatureExtractionCheckpoint? {
        let sql = """
            SELECT analysisAssetId, lastWindowStartTime, lastWindowEndTime, lastRms,
                   lastMusicProbability, lastRawSpeakerChangeProxyScore,
                   penultimateRawSpeakerChangeProxyScore, lastMagnitudesJSON, featureVersion
            FROM feature_extraction_state
            WHERE analysisAssetId = ? AND featureVersion = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        bind(stmt, 2, featureVersion)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        let checkpoint = try readFeatureExtractionCheckpoint(stmt)
        guard abs(checkpoint.lastWindowEndTime - endTime) <= 1e-6 else { return nil }
        return checkpoint
    }

    #if DEBUG
    private func triggerFeatureBatchPersistenceFaultIfNeeded(
        _ injection: FeatureBatchPersistenceFaultInjection
    ) throws {
        guard featureBatchPersistenceFaultInjection == injection else { return }
        featureBatchPersistenceFaultInjection = nil
        throw AnalysisStoreError.insertFailed(
            "Injected feature extraction batch persistence failure at \(injection)"
        )
    }

    /// playhead-uhdu (5uvz.1 NIT #1): one-shot fault trigger consumed by
    /// `acquireLeaseWithJournal`. Single-fire by design — clears itself
    /// so retries succeed without the test having to reset state.
    private func triggerLeaseJournalFaultIfNeeded(
        _ injection: LeaseJournalFaultInjection
    ) throws {
        guard leaseJournalFaultInjection == injection else { return }
        leaseJournalFaultInjection = nil
        throw AnalysisStoreError.insertFailed(
            "Injected lease-with-journal failure at \(injection)"
        )
    }

    /// playhead-5uvz.3 (Gap-3): one-shot fault trigger called inside the
    /// `processJob` outcome-arm transaction. Same single-fire semantics
    /// as `triggerLeaseJournalFaultIfNeeded`: it clears itself so a retry
    /// (e.g. orphan recovery's requeue) does not re-trip the fault.
    func triggerProcessJobOutcomeFaultIfNeeded(
        _ injection: ProcessJobOutcomeFaultInjection
    ) throws {
        guard processJobOutcomeFaultInjection == injection else { return }
        processJobOutcomeFaultInjection = nil
        throw AnalysisStoreError.insertFailed(
            "Injected processJob outcome-arm failure at \(injection)"
        )
    }
    #endif

    #if DEBUG
    /// Test-only call log of `fetchFeatureWindows` invocations, captured as
    /// `(assetId, from, to)` tuples in call order. Used by
    /// `RegionShadowPhaseIntegrationTests` to pin that the Phase 4 shadow
    /// phase's full-episode fetch does NOT occur when no observer is
    /// injected. Never read in production code.
    var fetchFeatureWindowsCallLog: [(assetId: String, from: Double, to: Double)] = []
    #endif

    func earliestFeatureWindowStart(
        assetId: String,
        before end: Double,
        earlierThanFeatureVersion version: Int
    ) throws -> Double? {
        let sql = """
            SELECT MIN(startTime)
            FROM feature_windows
            WHERE analysisAssetId = ? AND endTime <= ? AND featureVersion < ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        bind(stmt, 2, end)
        bind(stmt, 3, version)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard sqlite3_column_type(stmt, 0) != SQLITE_NULL else { return nil }
        return sqlite3_column_double(stmt, 0)
    }

    func fetchFeatureWindows(
        assetId: String,
        from start: Double,
        to end: Double,
        minimumFeatureVersion: Int? = FeatureExtractionConfig.default.featureVersion
    ) throws -> [FeatureWindow] {
        #if DEBUG
        fetchFeatureWindowsCallLog.append((assetId: assetId, from: start, to: end))
        #endif
        let versionClause = minimumFeatureVersion == nil ? "" : "AND featureVersion >= ?"
        let sql = """
            SELECT analysisAssetId, startTime, endTime, rms, spectralFlux,
                   musicProbability, speakerChangeProxyScore, musicBedChangeScore,
                   musicBedOnsetScore, musicBedOffsetScore, musicBedLevelRaw,
                   pauseProbability, speakerClusterId, jingleHash, featureVersion
            FROM feature_windows
            WHERE analysisAssetId = ? AND startTime >= ? AND endTime <= ? \(versionClause)
            ORDER BY startTime
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        bind(stmt, 2, start)
        bind(stmt, 3, end)
        if let minimumFeatureVersion {
            bind(stmt, 4, minimumFeatureVersion)
        }
        var results: [FeatureWindow] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readFeatureWindow(stmt))
        }
        return results
    }

    // MARK: - CRUD: transcript_chunks

    func insertTranscriptChunk(_ chunk: TranscriptChunk) throws {
        let sql = """
            INSERT INTO transcript_chunks
            (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime,
             text, normalizedText, pass, modelVersion, transcriptVersion, atomOrdinal,
             weakAnchorMetadataJSON, speakerId)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, chunk.id)
        bind(stmt, 2, chunk.analysisAssetId)
        bind(stmt, 3, chunk.segmentFingerprint)
        bind(stmt, 4, chunk.chunkIndex)
        bind(stmt, 5, chunk.startTime)
        bind(stmt, 6, chunk.endTime)
        bind(stmt, 7, chunk.text)
        bind(stmt, 8, chunk.normalizedText)
        bind(stmt, 9, chunk.pass)
        bind(stmt, 10, chunk.modelVersion)
        bind(stmt, 11, chunk.transcriptVersion)
        bind(stmt, 12, chunk.atomOrdinal)
        bind(stmt, 13, try encodeJSONString(chunk.weakAnchorMetadata))
        bind(stmt, 14, chunk.speakerId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func insertTranscriptChunks(_ chunks: [TranscriptChunk]) throws {
        guard !chunks.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for chunk in chunks {
                try insertTranscriptChunk(chunk)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func updateFastTranscriptCoverage(id: String, endTime: Double) throws {
        let sql = "UPDATE analysis_assets SET fastTranscriptCoverageEndTime = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, endTime)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func hasTranscriptChunk(analysisAssetId: String, segmentFingerprint: String) throws -> Bool {
        let sql = "SELECT 1 FROM transcript_chunks WHERE analysisAssetId = ? AND segmentFingerprint = ? LIMIT 1"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        bind(stmt, 2, segmentFingerprint)
        return sqlite3_step(stmt) == SQLITE_ROW
    }

    func fetchTranscriptChunk(
        analysisAssetId: String,
        segmentFingerprint: String
    ) throws -> TranscriptChunk? {
        let sql = """
            SELECT * FROM transcript_chunks
            WHERE analysisAssetId = ? AND segmentFingerprint = ?
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        bind(stmt, 2, segmentFingerprint)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readTranscriptChunk(stmt)
    }

    @discardableResult
    func updateTranscriptChunkWeakAnchorMetadata(
        analysisAssetId: String,
        segmentFingerprint: String,
        weakAnchorMetadata: TranscriptWeakAnchorMetadata?
    ) throws -> Bool {
        let sql = """
            UPDATE transcript_chunks
            SET weakAnchorMetadataJSON = ?
            WHERE analysisAssetId = ? AND segmentFingerprint = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, try encodeJSONString(weakAnchorMetadata))
        bind(stmt, 2, analysisAssetId)
        bind(stmt, 3, segmentFingerprint)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
    }

    func fetchTranscriptChunks(assetId: String) throws -> [TranscriptChunk] {
        let sql = "SELECT * FROM transcript_chunks WHERE analysisAssetId = ? ORDER BY chunkIndex"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        var results: [TranscriptChunk] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readTranscriptChunk(stmt))
        }
        return results
    }

    func searchTranscripts(query: String) throws -> [TranscriptChunk] {
        // Sanitize the query for FTS5: strip double quotes, then wrap each
        // whitespace-separated token in double quotes so special characters
        // (*, AND, OR, NEAR, etc.) are treated as literal search terms.
        let sanitized = query
            .replacingOccurrences(of: "\"", with: "")
            .split(whereSeparator: \.isWhitespace)
            .map { "\"\($0)\"" }
            .joined(separator: " ")
        guard !sanitized.isEmpty else { return [] }

        let sql = """
            SELECT tc.* FROM transcript_chunks tc
            JOIN transcript_chunks_fts fts ON tc.rowid = fts.rowid
            WHERE transcript_chunks_fts MATCH ?
            ORDER BY rank
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, sanitized)
        var results: [TranscriptChunk] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readTranscriptChunk(stmt))
        }
        return results
    }

    private func readTranscriptChunk(_ stmt: OpaquePointer?) -> TranscriptChunk {
        TranscriptChunk(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            segmentFingerprint: text(stmt, 2),
            chunkIndex: Int(sqlite3_column_int(stmt, 3)),
            startTime: sqlite3_column_double(stmt, 4),
            endTime: sqlite3_column_double(stmt, 5),
            text: text(stmt, 6),
            normalizedText: text(stmt, 7),
            pass: text(stmt, 8),
            modelVersion: text(stmt, 9),
            transcriptVersion: optionalText(stmt, 10),
            atomOrdinal: optionalInt(stmt, 11),
            weakAnchorMetadata: try? decodeJSON(
                TranscriptWeakAnchorMetadata.self,
                from: optionalText(stmt, 12)
            ),
            speakerId: optionalInt(stmt, 13)
        )
    }

    // MARK: - CRUD: ad_windows

    func insertAdWindow(_ ad: AdWindow) throws {
        // Column positions (1-indexed): id=1 analysisAssetId=2 startTime=3 endTime=4
        // confidence=5 boundaryState=6 decisionState=7 detectorVersion=8 advertiser=9
        // product=10 adDescription=11 evidenceText=12 evidenceStartTime=13
        // metadataSource=14 metadataConfidence=15 metadataPromptVersion=16 wasSkipped=17
        // userDismissedBanner=18 evidenceSources=19 eligibilityGate=20
        // catalogStoreMatchSimilarity=21 (playhead-epfk)
        // Keep bind() call indices and this comment in sync when adding columns.
        let sql = """
            INSERT INTO ad_windows
            (id, analysisAssetId, startTime, endTime, confidence, boundaryState,
             decisionState, detectorVersion, advertiser, product, adDescription,
             evidenceText, evidenceStartTime, metadataSource, metadataConfidence,
             metadataPromptVersion, wasSkipped, userDismissedBanner,
             evidenceSources, eligibilityGate, catalogStoreMatchSimilarity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, ad.id)
        bind(stmt, 2, ad.analysisAssetId)
        bind(stmt, 3, ad.startTime)
        bind(stmt, 4, ad.endTime)
        bind(stmt, 5, ad.confidence)
        bind(stmt, 6, ad.boundaryState)
        bind(stmt, 7, ad.decisionState)
        bind(stmt, 8, ad.detectorVersion)
        bind(stmt, 9, ad.advertiser)
        bind(stmt, 10, ad.product)
        bind(stmt, 11, ad.adDescription)
        bind(stmt, 12, ad.evidenceText)
        bind(stmt, 13, ad.evidenceStartTime)
        bind(stmt, 14, ad.metadataSource)
        bind(stmt, 15, ad.metadataConfidence)
        bind(stmt, 16, ad.metadataPromptVersion)
        bind(stmt, 17, ad.wasSkipped ? 1 : 0)
        bind(stmt, 18, ad.userDismissedBanner ? 1 : 0)
        bind(stmt, 19, ad.evidenceSources)
        bind(stmt, 20, ad.eligibilityGate)
        bind(stmt, 21, ad.catalogStoreMatchSimilarity)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchAdWindows(assetId: String) throws -> [AdWindow] {
        let sql = "SELECT * FROM ad_windows WHERE analysisAssetId = ? ORDER BY startTime"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        var results: [AdWindow] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(AdWindow(
                id: text(stmt, 0),
                analysisAssetId: text(stmt, 1),
                startTime: sqlite3_column_double(stmt, 2),
                endTime: sqlite3_column_double(stmt, 3),
                confidence: sqlite3_column_double(stmt, 4),
                boundaryState: text(stmt, 5),
                decisionState: text(stmt, 6),
                detectorVersion: text(stmt, 7),
                advertiser: optionalText(stmt, 8),
                product: optionalText(stmt, 9),
                adDescription: optionalText(stmt, 10),
                evidenceText: optionalText(stmt, 11),
                evidenceStartTime: optionalDouble(stmt, 12),
                metadataSource: text(stmt, 13),
                metadataConfidence: optionalDouble(stmt, 14),
                metadataPromptVersion: optionalText(stmt, 15),
                wasSkipped: sqlite3_column_int(stmt, 16) != 0,
                userDismissedBanner: sqlite3_column_int(stmt, 17) != 0,
                evidenceSources: optionalText(stmt, 18),
                eligibilityGate: optionalText(stmt, 19),
                // playhead-epfk: column 20 is the new
                // `catalogStoreMatchSimilarity` (REAL). Pre-epfk DBs that
                // run the migration get NULL → optionalDouble returns nil.
                catalogStoreMatchSimilarity: optionalDouble(stmt, 20)
            ))
        }
        return results
    }

    func updateAdWindowDecision(id: String, decisionState: String) throws {
        let sql = "UPDATE ad_windows SET decisionState = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, decisionState)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateAdWindowHotPathCandidate(_ ad: AdWindow) throws {
        let sql = """
            UPDATE ad_windows SET
                startTime = ?, endTime = ?, confidence = ?, boundaryState = ?,
                evidenceText = ?, evidenceStartTime = ?, evidenceSources = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, ad.startTime)
        bind(stmt, 2, ad.endTime)
        bind(stmt, 3, ad.confidence)
        bind(stmt, 4, ad.boundaryState)
        bind(stmt, 5, ad.evidenceText)
        bind(stmt, 6, ad.evidenceStartTime)
        bind(stmt, 7, ad.evidenceSources)
        bind(stmt, 8, ad.id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func insertAdWindows(_ windows: [AdWindow]) throws {
        guard !windows.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for ad in windows {
                try insertAdWindow(ad)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func upsertHotPathAdWindows(
        _ windows: [AdWindow],
        existingIDs: Set<String>,
        retiredIDs: Set<String> = []
    ) throws {
        guard !windows.isEmpty || !retiredIDs.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for ad in windows {
                if existingIDs.contains(ad.id) {
                    try updateAdWindowHotPathCandidate(ad)
                } else {
                    try insertAdWindow(ad)
                }
            }
            if !retiredIDs.isEmpty {
                try deleteAdWindows(ids: retiredIDs)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    private func deleteAdWindows(ids: Set<String>) throws {
        guard !ids.isEmpty else { return }
        let sql = "DELETE FROM ad_windows WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        for id in ids {
            sqlite3_reset(stmt)
            sqlite3_clear_bindings(stmt)
            bind(stmt, 1, id)
            try step(stmt, expecting: SQLITE_DONE)
        }
    }

    func updateAdWindowMetadata(
        id: String,
        advertiser: String?,
        product: String?,
        evidenceText: String?,
        metadataSource: String,
        metadataConfidence: Double?,
        metadataPromptVersion: String?
    ) throws {
        let sql = """
            UPDATE ad_windows SET
                advertiser = ?, product = ?, evidenceText = ?,
                metadataSource = ?, metadataConfidence = ?, metadataPromptVersion = ?
            WHERE id = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, advertiser)
        bind(stmt, 2, product)
        bind(stmt, 3, evidenceText)
        bind(stmt, 4, metadataSource)
        bind(stmt, 5, metadataConfidence)
        bind(stmt, 6, metadataPromptVersion)
        bind(stmt, 7, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateAdWindowWasSkipped(id: String, wasSkipped: Bool) throws {
        let sql = "UPDATE ad_windows SET wasSkipped = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, wasSkipped ? 1 : 0)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateConfirmedAdCoverage(id: String, endTime: Double) throws {
        let sql = "UPDATE analysis_assets SET confirmedAdCoverageEndTime = ? WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, endTime)
        bind(stmt, 2, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchAllFeatureWindows(
        assetId: String,
        minimumFeatureVersion: Int? = FeatureExtractionConfig.default.featureVersion
    ) throws -> [FeatureWindow] {
        let versionClause = minimumFeatureVersion == nil ? "" : "AND featureVersion >= ?"
        let sql = """
            SELECT analysisAssetId, startTime, endTime, rms, spectralFlux,
                   musicProbability, speakerChangeProxyScore, musicBedChangeScore,
                   musicBedOnsetScore, musicBedOffsetScore, musicBedLevelRaw,
                   pauseProbability, speakerClusterId, jingleHash, featureVersion
            FROM feature_windows
            WHERE analysisAssetId = ? \(versionClause)
            ORDER BY startTime
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        if let minimumFeatureVersion {
            bind(stmt, 2, minimumFeatureVersion)
        }
        var results: [FeatureWindow] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readFeatureWindow(stmt))
        }
        return results
    }

    private func readFeatureWindow(_ stmt: OpaquePointer?) -> FeatureWindow {
        let levelRaw = optionalText(stmt, 10) ?? "none"
        let level = MusicBedLevel(rawValue: levelRaw) ?? .none
        return FeatureWindow(
            analysisAssetId: text(stmt, 0),
            startTime: sqlite3_column_double(stmt, 1),
            endTime: sqlite3_column_double(stmt, 2),
            rms: sqlite3_column_double(stmt, 3),
            spectralFlux: sqlite3_column_double(stmt, 4),
            musicProbability: sqlite3_column_double(stmt, 5),
            speakerChangeProxyScore: sqlite3_column_double(stmt, 6),
            musicBedChangeScore: sqlite3_column_double(stmt, 7),
            musicBedOnsetScore: sqlite3_column_double(stmt, 8),
            musicBedOffsetScore: sqlite3_column_double(stmt, 9),
            musicBedLevel: level,
            pauseProbability: sqlite3_column_double(stmt, 11),
            speakerClusterId: optionalInt(stmt, 12),
            jingleHash: optionalText(stmt, 13),
            featureVersion: Int(sqlite3_column_int(stmt, 14))
        )
    }

    // MARK: - CRUD: skip_cues

    func insertSkipCue(_ cue: SkipCue) throws {
        let sql = """
            INSERT OR IGNORE INTO skip_cues
            (id, analysisAssetId, cueHash, startTime, endTime, confidence,
             source, materializedAt, wasSkipped, userDismissed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, cue.id)
        bind(stmt, 2, cue.analysisAssetId)
        bind(stmt, 3, cue.cueHash)
        bind(stmt, 4, cue.startTime)
        bind(stmt, 5, cue.endTime)
        bind(stmt, 6, cue.confidence)
        bind(stmt, 7, cue.source)
        bind(stmt, 8, cue.materializedAt)
        bind(stmt, 9, cue.wasSkipped ? 1 : 0)
        bind(stmt, 10, cue.userDismissed ? 1 : 0)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func insertSkipCues(_ cues: [SkipCue]) throws {
        guard !cues.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for cue in cues {
                try insertSkipCue(cue)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func fetchSkipCues(for analysisAssetId: String) throws -> [SkipCue] {
        let sql = "SELECT * FROM skip_cues WHERE analysisAssetId = ? ORDER BY startTime ASC"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [SkipCue] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readSkipCue(stmt))
        }
        return results
    }

    func markSkipCueSkipped(id: String) throws {
        let sql = "UPDATE skip_cues SET wasSkipped = 1 WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func markSkipCueDismissed(id: String) throws {
        let sql = "UPDATE skip_cues SET userDismissed = 1 WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        try step(stmt, expecting: SQLITE_DONE)
    }

    private func readSkipCue(_ stmt: OpaquePointer?) -> SkipCue {
        SkipCue(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            cueHash: text(stmt, 2),
            startTime: sqlite3_column_double(stmt, 3),
            endTime: sqlite3_column_double(stmt, 4),
            confidence: sqlite3_column_double(stmt, 5),
            source: text(stmt, 6),
            materializedAt: sqlite3_column_double(stmt, 7),
            wasSkipped: sqlite3_column_int(stmt, 8) != 0,
            userDismissed: sqlite3_column_int(stmt, 9) != 0
        )
    }

    // MARK: - CRUD: podcast_profiles

    func upsertProfile(_ profile: PodcastProfile) throws {
        // playhead-i9dj: `title` participates in the upsert with
        // COALESCE-preserve semantics — a profile rebuild that doesn't
        // know the title (e.g. TrustScoringService re-upserting a
        // fetched profile) must NOT clobber a previously-recorded
        // title. `COALESCE(excluded.title, podcast_profiles.title)`
        // keeps the existing column when the new write is NULL and
        // overwrites only when the new write carries a non-NULL value.
        let sql = """
            INSERT INTO podcast_profiles
            (podcastId, sponsorLexicon, normalizedAdSlotPriors, repeatedCTAFragments,
             jingleFingerprints, implicitFalsePositiveCount, skipTrustScore,
             observationCount, mode, recentFalseSkipSignals, traitProfileJSON,
             title)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(podcastId) DO UPDATE SET
                sponsorLexicon = excluded.sponsorLexicon,
                normalizedAdSlotPriors = excluded.normalizedAdSlotPriors,
                repeatedCTAFragments = excluded.repeatedCTAFragments,
                jingleFingerprints = excluded.jingleFingerprints,
                implicitFalsePositiveCount = excluded.implicitFalsePositiveCount,
                skipTrustScore = excluded.skipTrustScore,
                observationCount = excluded.observationCount,
                mode = excluded.mode,
                recentFalseSkipSignals = excluded.recentFalseSkipSignals,
                traitProfileJSON = excluded.traitProfileJSON,
                title = COALESCE(excluded.title, podcast_profiles.title)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, profile.podcastId)
        bind(stmt, 2, profile.sponsorLexicon)
        bind(stmt, 3, profile.normalizedAdSlotPriors)
        bind(stmt, 4, profile.repeatedCTAFragments)
        bind(stmt, 5, profile.jingleFingerprints)
        bind(stmt, 6, profile.implicitFalsePositiveCount)
        bind(stmt, 7, profile.skipTrustScore)
        bind(stmt, 8, profile.observationCount)
        bind(stmt, 9, profile.mode)
        bind(stmt, 10, profile.recentFalseSkipSignals)
        bind(stmt, 11, profile.traitProfileJSON)
        bind(stmt, 12, profile.title)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchProfile(podcastId: String) throws -> PodcastProfile? {
        // playhead-i9dj: explicit column list (not `SELECT *`) so future
        // additive migrations don't shift the positional indices the
        // decoder reads. `title` lands at index 11 — append-only.
        let sql = """
            SELECT podcastId, sponsorLexicon, normalizedAdSlotPriors, repeatedCTAFragments,
                   jingleFingerprints, implicitFalsePositiveCount, skipTrustScore,
                   observationCount, mode, recentFalseSkipSignals, traitProfileJSON,
                   title
            FROM podcast_profiles WHERE podcastId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return PodcastProfile(
            podcastId: text(stmt, 0),
            sponsorLexicon: optionalText(stmt, 1),
            normalizedAdSlotPriors: optionalText(stmt, 2),
            repeatedCTAFragments: optionalText(stmt, 3),
            jingleFingerprints: optionalText(stmt, 4),
            implicitFalsePositiveCount: Int(sqlite3_column_int(stmt, 5)),
            skipTrustScore: sqlite3_column_double(stmt, 6),
            observationCount: Int(sqlite3_column_int(stmt, 7)),
            mode: text(stmt, 8),
            recentFalseSkipSignals: Int(sqlite3_column_int(stmt, 9)),
            traitProfileJSON: optionalText(stmt, 10),
            title: optionalText(stmt, 11)
        )
    }

    /// playhead-i9dj: persist the human-readable show title on the
    /// `podcast_profiles` row so an exported `analysis.sqlite` is
    /// legible without joining to the SwiftData side.
    ///
    /// Lazy-create + idempotent semantics:
    ///   * If no profile row exists for `podcastId`, this method is a
    ///     no-op (the title will land on the next `upsertProfile` from
    ///     trust-scoring once the title parameter is threaded — until
    ///     then we don't materialize a title-only stub).
    ///   * If a profile row exists, the title column is overwritten
    ///     with the supplied value. Passing `nil` is a no-op (preserves
    ///     any previously-recorded title) — same conservative semantics
    ///     as ``updateAssetEpisodeTitle(id:episodeTitle:)``.
    ///
    /// - Parameters:
    ///   - podcastId: `podcast_profiles.podcastId` of the row to update.
    ///   - title: the show title to persist, or `nil` for a no-op.
    func updateProfileTitle(podcastId: String, title: String?) throws {
        guard let title else { return }

        let sql = "UPDATE podcast_profiles SET title = ? WHERE podcastId = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, title)
        bind(stmt, 2, podcastId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    // MARK: - CRUD: podcast_planner_state (bd-m8k)

    /// bd-m8k: Returns the persisted `PodcastPlannerState` for `podcastId`, or
    /// `nil` if no row has been created for this podcast yet. Callers should
    /// treat `nil` as the conservative cold-start default
    /// (`observedEpisodeCount = 0`, `stablePrecisionFlag = false`,
    /// `episodesSinceLastFullRescan = 0`) — the migration deliberately leaves
    /// the table empty and rows are created lazily on first observation.
    func fetchPodcastPlannerState(podcastId: String) throws -> PodcastPlannerState? {
        // historical: stored as "precision*"; semantically recall
        // Cycle 4 B4: two new persisted counters appended at the end.
        let sql = """
            SELECT podcastId,
                   observedEpisodeCount,
                   episodesSinceLastFullRescan,
                   stablePrecisionFlag,
                   lastFullRescanAt,
                   precisionSample1,
                   precisionSample2,
                   precisionSample3,
                   precisionSampleCount,
                   episodesObservedWithoutSampleCount,
                   narrowingAllPhasesEmptyEpisodeCount
            FROM podcast_planner_state
            WHERE podcastId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }

        // Cycle 2 Rev4-M3: clamp the persisted sample count into the valid
        // range and log loudly when the clamp fires. A row that has
        // `precisionSampleCount` outside `[0, plannerRecallRingSize]` is
        // either a bug, a manual SQL edit, or a corrupted file — either way
        // operators should see it in Console.app instead of the store
        // silently rounding past it.
        let rawSampleCount = Int(sqlite3_column_int(stmt, 8))
        let sampleCount = max(0, min(Self.plannerRecallRingSize, rawSampleCount))
        if rawSampleCount != sampleCount {
            logger.error(
                "podcast_planner_state.precisionSampleCount=\(rawSampleCount, privacy: .public) out of range [0, \(Self.plannerRecallRingSize, privacy: .public)] for podcast=\(podcastId, privacy: .public); clamped to \(sampleCount, privacy: .public)"
            )
        }
        // Samples are stored oldest → newest in columns 5/6/7. We hand back
        // exactly `sampleCount` doubles so callers cannot accidentally treat
        // a NULL slot as a real measurement.
        var samples: [Double] = []
        samples.reserveCapacity(sampleCount)
        for offset in 0..<sampleCount {
            if let value = optionalDouble(stmt, Int32(5 + offset)) {
                samples.append(value)
            }
        }

        // Cycle 4 B4: columns 9/10 are the Cycle-4 additions. Legacy rows
        // default-decode to 0 thanks to `DEFAULT 0` on both columns —
        // SQLite hands back the column default for NULL-absent reads.
        let episodesObservedWithoutSampleCount = Int(sqlite3_column_int(stmt, 9))
        let narrowingAllPhasesEmptyEpisodeCount = Int(sqlite3_column_int(stmt, 10))

        return PodcastPlannerState(
            podcastId: text(stmt, 0),
            observedEpisodeCount: Int(sqlite3_column_int(stmt, 1)),
            episodesSinceLastFullRescan: Int(sqlite3_column_int(stmt, 2)),
            // historical: stored as "stablePrecisionFlag"; semantically recall
            stableRecallFlag: sqlite3_column_int(stmt, 3) != 0,
            lastFullRescanAt: optionalDouble(stmt, 4),
            // historical: stored as "precisionSamples"; semantically recall
            recallSamples: samples,
            episodesObservedWithoutSampleCount: episodesObservedWithoutSampleCount,
            narrowingAllPhasesEmptyEpisodeCount: narrowingAllPhasesEmptyEpisodeCount
        )
    }

    /// bd-m8k: Records that a backfill pass for `podcastId` has just completed
    /// and returns the updated state.
    ///
    /// **Lazy creation:** if no row exists for `podcastId`, one is inserted at
    /// cold-start defaults before the bookkeeping below is applied. This is
    /// the only path that materializes a row — there is no migration backfill
    /// and no separate `upsert` API.
    ///
    /// **Bookkeeping rules** (per the bd-m8k design field):
    /// - `observedEpisodeCount` is incremented by 1 on every call.
    /// - `wasFullRescan == true`: `episodesSinceLastFullRescan` resets to 0,
    ///   `lastFullRescanAt` is updated, and (when `fullRescanPrecisionSample`
    ///   is non-nil) the sample is appended to the recall ring with the
    ///   oldest entry dropped if the ring is already full.
    /// - `wasFullRescan == false`: `episodesSinceLastFullRescan` is
    ///   incremented; the recall ring is left untouched. A recall
    ///   sample passed alongside a non-full-rescan call is ignored (the
    ///   targeted-with-audit pass cannot measure recall against itself).
    /// - `stableRecallFlag` is recomputed from the post-update state on
    ///   every call: it is true iff
    ///   `observedEpisodeCount >= plannerStableObservedEpisodeFloor` AND the
    ///   ring is full (`plannerRecallRingSize` samples) AND every sample
    ///   in the ring is `>= plannerRecallThreshold`. If any condition
    ///   fails the flag is forced false, even if a previous write set it to
    ///   true (the ring shrinks back to false on regression).
    /// - Cycle 4 B4: `incrementEpisodesObservedWithoutSample` and
    ///   `incrementNarrowingAllPhasesEmpty` are independent per-podcast
    ///   counters. When true, the persisted counters are read-modify-written
    ///   under the same transaction as the rest of the bookkeeping. Both
    ///   flags are orthogonal — an ad-free full rescan passes
    ///   `incrementEpisodesObservedWithoutSample = true` and an all-phases-
    ///   empty targeted run passes `incrementNarrowingAllPhasesEmpty = true`.
    ///   A full rescan can pass both (ad-free episode where narrowing was
    ///   also empty).
    @discardableResult
    func recordPodcastEpisodeObservation(
        podcastId: String,
        wasFullRescan: Bool,
        fullRescanPrecisionSample: Double? = nil,
        incrementEpisodesObservedWithoutSample: Bool = false,
        incrementNarrowingAllPhasesEmpty: Bool = false,
        now: Double
    ) throws -> PodcastPlannerState {
        // Wrap the read-modify-write in a transaction so a concurrent
        // observation for the same podcast cannot interleave a stale read
        // with our write. SQLite's busy_timeout already serializes writers,
        // but BEGIN IMMEDIATE upgrades the lock immediately so two callers
        // hitting the same row see SQLITE_BUSY rather than racing on the
        // counter.
        try exec("BEGIN IMMEDIATE")
        do {
            let prior = try fetchPodcastPlannerState(podcastId: podcastId)
            // historical: stored as "precision*"; semantically recall
            let priorSamples = prior?.recallSamples ?? []

            let newObservedCount = (prior?.observedEpisodeCount ?? 0) + 1
            let newEpisodesSince: Int
            let newLastFullRescanAt: Double?
            var newSamples = priorSamples

            if wasFullRescan {
                newEpisodesSince = 0
                newLastFullRescanAt = now
                // Cycle 2 C4: parameter is named `fullRescanPrecisionSample`
                // for legacy compatibility but the value semantically is a
                // recall sample. Ad-free episodes pass nil and the ring is
                // intentionally NOT advanced (no fake 1.0).
                if let sample = fullRescanPrecisionSample {
                    newSamples.append(sample)
                    while newSamples.count > Self.plannerRecallRingSize {
                        newSamples.removeFirst()
                    }
                }
            } else {
                newEpisodesSince = (prior?.episodesSinceLastFullRescan ?? 0) + 1
                newLastFullRescanAt = prior?.lastFullRescanAt
                // Intentionally do NOT touch the recall ring on
                // non-full-rescan observations — see doc comment above.
            }

            let stableFlag = Self.computePlannerStableFlag(
                observedEpisodeCount: newObservedCount,
                samples: newSamples
            )

            // Cycle 4 B4: per-podcast counters. Read prior value (0 for
            // missing rows via the struct default above) and bump under
            // the same BEGIN IMMEDIATE that guards the rest of the
            // bookkeeping.
            let newEpisodesObservedWithoutSample =
                (prior?.episodesObservedWithoutSampleCount ?? 0)
                + (incrementEpisodesObservedWithoutSample ? 1 : 0)
            let newNarrowingAllPhasesEmptyEpisodes =
                (prior?.narrowingAllPhasesEmptyEpisodeCount ?? 0)
                + (incrementNarrowingAllPhasesEmpty ? 1 : 0)

            try writePodcastPlannerStateRow(
                podcastId: podcastId,
                observedEpisodeCount: newObservedCount,
                episodesSinceLastFullRescan: newEpisodesSince,
                stableRecallFlag: stableFlag,
                lastFullRescanAt: newLastFullRescanAt,
                samples: newSamples,
                episodesObservedWithoutSampleCount: newEpisodesObservedWithoutSample,
                narrowingAllPhasesEmptyEpisodeCount: newNarrowingAllPhasesEmptyEpisodes
            )

            try exec("COMMIT")

            return PodcastPlannerState(
                podcastId: podcastId,
                observedEpisodeCount: newObservedCount,
                episodesSinceLastFullRescan: newEpisodesSince,
                // historical: stored as "stablePrecisionFlag"; semantically recall
                stableRecallFlag: stableFlag,
                lastFullRescanAt: newLastFullRescanAt,
                // historical: stored as "precisionSamples"; semantically recall
                recallSamples: newSamples,
                episodesObservedWithoutSampleCount: newEpisodesObservedWithoutSample,
                narrowingAllPhasesEmptyEpisodeCount: newNarrowingAllPhasesEmptyEpisodes
            )
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// bd-m8k: pure helper exposed for tests. Computes the stable-recall
    /// flag from a post-update `(observedEpisodeCount, samples)` tuple. The
    /// flag is true iff:
    /// 1. `observedEpisodeCount >= plannerStableObservedEpisodeFloor` (5), AND
    /// 2. The recall ring contains exactly `plannerRecallRingSize` (3)
    ///    samples, AND
    /// 3. Every sample is `>= plannerRecallThreshold` (0.85).
    ///
    /// The "exactly 3 samples" requirement is deliberate: a freshly
    /// observed podcast with one stellar recall sample must not flip the
    /// flag — we want at least three full-rescan recall measurements
    /// before trusting the targeted-with-audit branch.
    nonisolated static func computePlannerStableFlag(
        observedEpisodeCount: Int,
        samples: [Double]
    ) -> Bool {
        guard observedEpisodeCount >= plannerStableObservedEpisodeFloor else { return false }
        guard samples.count >= plannerRecallRingSize else { return false }
        return samples.allSatisfy { $0 >= plannerRecallThreshold }
    }

    private func writePodcastPlannerStateRow(
        podcastId: String,
        observedEpisodeCount: Int,
        episodesSinceLastFullRescan: Int,
        // Cycle 6 B6 L: parameter name follows the "recall" semantic the
        // cycle-4 rename pass established. The underlying SQLite column is
        // still `stablePrecisionFlag` for backwards compatibility.
        stableRecallFlag: Bool,
        lastFullRescanAt: Double?,
        samples: [Double],
        episodesObservedWithoutSampleCount: Int,
        narrowingAllPhasesEmptyEpisodeCount: Int
    ) throws {
        // Pad the samples array out to the fixed-width ring slots so we can
        // unconditionally bind 3 columns regardless of how many samples we
        // have in hand.
        var ring: [Double?] = Array(repeating: nil, count: Self.plannerRecallRingSize)
        for (idx, value) in samples.enumerated()
        where idx < Self.plannerRecallRingSize {
            ring[idx] = value
        }

        // Cycle 4 B4: two new persisted counters appended.
        let sql = """
            INSERT INTO podcast_planner_state
            (podcastId, observedEpisodeCount, episodesSinceLastFullRescan,
             stablePrecisionFlag, lastFullRescanAt,
             precisionSample1, precisionSample2, precisionSample3,
             precisionSampleCount,
             episodesObservedWithoutSampleCount,
             narrowingAllPhasesEmptyEpisodeCount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(podcastId) DO UPDATE SET
                observedEpisodeCount                = excluded.observedEpisodeCount,
                episodesSinceLastFullRescan         = excluded.episodesSinceLastFullRescan,
                stablePrecisionFlag                 = excluded.stablePrecisionFlag,
                lastFullRescanAt                    = excluded.lastFullRescanAt,
                precisionSample1                    = excluded.precisionSample1,
                precisionSample2                    = excluded.precisionSample2,
                precisionSample3                    = excluded.precisionSample3,
                precisionSampleCount                = excluded.precisionSampleCount,
                episodesObservedWithoutSampleCount  = excluded.episodesObservedWithoutSampleCount,
                narrowingAllPhasesEmptyEpisodeCount = excluded.narrowingAllPhasesEmptyEpisodeCount
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, observedEpisodeCount)
        bind(stmt, 3, episodesSinceLastFullRescan)
        bind(stmt, 4, stableRecallFlag ? 1 : 0)
        bind(stmt, 5, lastFullRescanAt)
        bind(stmt, 6, ring[0])
        bind(stmt, 7, ring[1])
        bind(stmt, 8, ring[2])
        bind(stmt, 9, samples.count)
        bind(stmt, 10, episodesObservedWithoutSampleCount)
        bind(stmt, 11, narrowingAllPhasesEmptyEpisodeCount)
        try step(stmt, expecting: SQLITE_DONE)
    }

    // MARK: - CRUD: preview_budgets

    func upsertBudget(_ budget: PreviewBudget) throws {
        let sql = """
            INSERT INTO preview_budgets
            (canonicalEpisodeKey, consumedAnalysisSeconds, graceBreakWindow, lastUpdated)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(canonicalEpisodeKey) DO UPDATE SET
                consumedAnalysisSeconds = excluded.consumedAnalysisSeconds,
                graceBreakWindow = excluded.graceBreakWindow,
                lastUpdated = excluded.lastUpdated
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, budget.canonicalEpisodeKey)
        bind(stmt, 2, budget.consumedAnalysisSeconds)
        bind(stmt, 3, budget.graceBreakWindow)
        bind(stmt, 4, budget.lastUpdated)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func fetchBudget(key: String) throws -> PreviewBudget? {
        let sql = "SELECT * FROM preview_budgets WHERE canonicalEpisodeKey = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, key)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return PreviewBudget(
            canonicalEpisodeKey: text(stmt, 0),
            consumedAnalysisSeconds: sqlite3_column_double(stmt, 1),
            graceBreakWindow: sqlite3_column_double(stmt, 2),
            lastUpdated: sqlite3_column_double(stmt, 3)
        )
    }

    // MARK: - CRUD: analysis_jobs

    @discardableResult
    func insertJob(_ job: AnalysisJob) throws -> Bool {
        // playhead-uzdq: generationID + schedulerEpoch are appended.
        // Defaults on the AnalysisJob init mean legacy callers that
        // don't pass them insert the "no-lease" sentinels ('' and 0),
        // matching the column DEFAULTs for pre-existing rows.
        let sql = """
            INSERT OR IGNORE INTO analysis_jobs
            (jobId, jobType, episodeId, podcastId, analysisAssetId, workKey,
             sourceFingerprint, downloadId, priority, desiredCoverageSec,
             featureCoverageSec, transcriptCoverageSec, cueCoverageSec,
             state, attemptCount, nextEligibleAt, leaseOwner, leaseExpiresAt,
             lastErrorCode, createdAt, updatedAt, generationID, schedulerEpoch)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, job.jobId)
        bind(stmt, 2, job.jobType)
        bind(stmt, 3, job.episodeId)
        bind(stmt, 4, job.podcastId)
        bind(stmt, 5, job.analysisAssetId)
        bind(stmt, 6, job.workKey)
        bind(stmt, 7, job.sourceFingerprint)
        bind(stmt, 8, job.downloadId)
        bind(stmt, 9, job.priority)
        bind(stmt, 10, job.desiredCoverageSec)
        bind(stmt, 11, job.featureCoverageSec)
        bind(stmt, 12, job.transcriptCoverageSec)
        bind(stmt, 13, job.cueCoverageSec)
        bind(stmt, 14, job.state)
        bind(stmt, 15, job.attemptCount)
        bind(stmt, 16, job.nextEligibleAt)
        bind(stmt, 17, job.leaseOwner)
        bind(stmt, 18, job.leaseExpiresAt)
        bind(stmt, 19, job.lastErrorCode)
        bind(stmt, 20, job.createdAt)
        bind(stmt, 21, job.updatedAt)
        bind(stmt, 22, job.generationID)
        bind(stmt, 23, job.schedulerEpoch)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
    }

    func fetchJob(byId jobId: String) throws -> AnalysisJob? {
        let sql = "SELECT * FROM analysis_jobs WHERE jobId = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readJob(stmt)
    }

    /// Returns the most-recently-updated `analysis_jobs` row for an
    /// episode. Primarily used by the playhead-44h1 foreground-assist
    /// hand-off so the BG task expiration / completion paths can
    /// resolve the current `{generationID, schedulerEpoch}` and append
    /// a terminal WorkJournal row keyed by the episode alone. Returns
    /// `nil` when the episode has no row.
    func fetchLatestJobForEpisode(_ episodeId: String) throws -> AnalysisJob? {
        let sql = """
            SELECT * FROM analysis_jobs
            WHERE episodeId = ?
            ORDER BY updatedAt DESC, rowid DESC
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readJob(stmt)
    }

    func fetchNextEligibleJob(
        deferredWorkAllowed: Bool,
        t0ThresholdSec: Double,
        now: TimeInterval
    ) throws -> AnalysisJob? {
        // T0 jobs: playback jobs that have zero coverage — always eligible.
        // Deferred jobs: backfill/preAnalysis require the caller's shared
        // admission-policy gate to allow deferred work and nextEligibleAt <=
        // now (or NULL).
        let sql = """
            SELECT * FROM analysis_jobs
            WHERE (
                (state IN ('queued', 'paused')
                  AND (leaseOwner IS NULL OR leaseExpiresAt < ?)
                  AND (nextEligibleAt IS NULL OR nextEligibleAt <= ?))
                OR (state = 'failed' AND nextEligibleAt IS NOT NULL AND nextEligibleAt <= ?)
              )
              AND (
                (jobType = 'playback' AND featureCoverageSec < ?)
                OR (
                  ? = 1
                  AND (nextEligibleAt IS NULL OR nextEligibleAt <= ?)
                )
              )
            ORDER BY priority DESC, createdAt ASC
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, now)
        bind(stmt, 2, now)
        bind(stmt, 3, now)
        bind(stmt, 4, t0ThresholdSec)
        bind(stmt, 5, deferredWorkAllowed ? 1 : 0)
        bind(stmt, 6, now)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return readJob(stmt)
    }

    func updateJobProgress(
        jobId: String,
        featureCoverageSec: Double,
        transcriptCoverageSec: Double,
        cueCoverageSec: Double
    ) throws {
        let sql = """
            UPDATE analysis_jobs
            SET featureCoverageSec = ?, transcriptCoverageSec = ?, cueCoverageSec = ?,
                updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, featureCoverageSec)
        bind(stmt, 2, transcriptCoverageSec)
        bind(stmt, 3, cueCoverageSec)
        bind(stmt, 4, Date().timeIntervalSince1970)
        bind(stmt, 5, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateJobState(jobId: String, state: String, nextEligibleAt: Double? = nil, lastErrorCode: String? = nil) throws {
        let sql = """
            UPDATE analysis_jobs
            SET state = ?, nextEligibleAt = ?, lastErrorCode = ?, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        bind(stmt, 2, nextEligibleAt)
        bind(stmt, 3, lastErrorCode)
        bind(stmt, 4, Date().timeIntervalSince1970)
        bind(stmt, 5, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func updateJobAnalysisAssetId(jobId: String, analysisAssetId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET analysisAssetId = ?, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// playhead-yqax: persist a new `desiredCoverageSec` on an
    /// `analysis_jobs` row. Used by the foreground catch-up bypass in
    /// `AnalysisWorkScheduler.dispatchForegroundCatchup` so the runner
    /// picks up the escalated coverage target on its next read of the
    /// row, AND so a crash mid-catch-up resumes against the deeper
    /// target rather than the stale tier value the row was enqueued
    /// with.
    ///
    /// Idempotent: writing the same value twice is a no-op for the
    /// runner. `updatedAt` always advances so the row's lease/lifecycle
    /// observers see the touch. Does NOT change `state`, `priority`,
    /// `attemptCount`, or any coverage-progress columns — those are
    /// owned by the standard outcome-arm transitions in
    /// `AnalysisWorkScheduler.processJob`.
    func updateJobDesiredCoverage(
        jobId: String,
        desiredCoverageSec: Double
    ) throws {
        let sql = """
            UPDATE analysis_jobs
            SET desiredCoverageSec = ?, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, desiredCoverageSec)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func acquireLease(jobId: String, owner: String, expiresAt: Double) throws -> Bool {
        let sql = """
            UPDATE analysis_jobs
            SET leaseOwner = ?, leaseExpiresAt = ?, state = 'running', updatedAt = ?
            WHERE jobId = ? AND (leaseOwner IS NULL OR leaseExpiresAt < ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        let now = Date().timeIntervalSince1970
        bind(stmt, 1, owner)
        bind(stmt, 2, expiresAt)
        bind(stmt, 3, now)
        bind(stmt, 4, jobId)
        bind(stmt, 5, now)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
    }

    /// playhead-5uvz.1 (Gap-1): jobId-keyed lease acquire that ALSO
    /// atomically appends a `work_journal` row with `eventType=.acquired`
    /// inside the same SQL transaction as the `analysis_jobs` UPDATE.
    ///
    /// This is the production-scheduler-friendly variant of
    /// ``acquireEpisodeLease``: same atomic-journal-append guarantee,
    /// keyed on `jobId` (the scheduler's primary handle) instead of
    /// `episodeId`, and it does NOT require the caller to supply a
    /// `schedulerEpoch` / `generationID` / `ownerWorkerId`. Those are
    /// minted/read inside the transaction so a worker that has not
    /// adopted the rich `EpisodeExecutionLease` API still produces a
    /// journal trail that ``AnalysisCoordinator/recoverOrphans`` can
    /// read.
    ///
    /// **Atomicity contract:** the lease columns and the journal row land
    /// in the same `BEGIN IMMEDIATE..COMMIT` envelope. If the journal
    /// append fails, the lease UPDATE rolls back — the caller sees a
    /// thrown error and **never holds a phantom lease without journal
    /// evidence**. This avoids the "lease held but `recoverOrphans`
    /// cannot route it" failure mode that prompted Gap-1.
    ///
    /// Return semantics match the bare ``acquireLease`` for drop-in
    /// substitution: `true` iff the row was both updated AND a journal
    /// row was appended (both inside the same committed transaction).
    /// Returns `false` when the lease slot was already taken (no UPDATE,
    /// no journal append, no transaction commit).
    ///
    /// **Behavioral divergence from the bare API:** `generationID` and
    /// `schedulerEpoch` rotate on every successful acquire here (the
    /// journal join in `recoverOrphans` requires them in sync between
    /// the row and its `.acquired` event). Callers that previously relied
    /// on identity preservation across reacquires must not assume drop-in
    /// equivalence on those two columns.
    ///
    /// `episodeId` is the only addition over the bare API — needed for
    /// the journal's primary key. The scheduler always has it on the
    /// job it just claimed.
    func acquireLeaseWithJournal(
        jobId: String,
        episodeId: String,
        owner: String,
        expiresAt: Double,
        now: Double = Date().timeIntervalSince1970,
        metadataJSON: String = "{}"
    ) throws -> Bool {
        try exec("BEGIN IMMEDIATE")
        do {
            // Mint a fresh generationID for this acquire and read the
            // store's current epoch. The journal row and the
            // analysis_jobs row are stamped with the same pair so
            // `recoverOrphans` can join them via
            // `fetchLastWorkJournalEntry(episodeId:generationID:)`.
            let generationID = UUID().uuidString
            let currentEpoch = try fetchSchedulerEpoch() ?? 0

            let updateSQL = """
                UPDATE analysis_jobs
                SET leaseOwner = ?, leaseExpiresAt = ?,
                    generationID = ?, schedulerEpoch = ?,
                    state = 'running', updatedAt = ?
                WHERE jobId = ? AND (leaseOwner IS NULL OR leaseExpiresAt < ?)
                """
            let stmt = try prepare(updateSQL)
            defer { sqlite3_finalize(stmt) }
            bind(stmt, 1, owner)
            bind(stmt, 2, expiresAt)
            bind(stmt, 3, generationID)
            bind(stmt, 4, currentEpoch)
            bind(stmt, 5, now)
            bind(stmt, 6, jobId)
            bind(stmt, 7, now)
            try step(stmt, expecting: SQLITE_DONE)
            if sqlite3_changes(db) == 0 {
                // Lease slot already taken — nothing to journal, no
                // state change to commit. Roll back the (empty) txn
                // and report no acquisition. No phantom row.
                try exec("ROLLBACK")
                return false
            }

            #if DEBUG
            try triggerLeaseJournalFaultIfNeeded(.afterUpdateBeforeJournalAppend)
            #endif

            // Atomically append the `acquired` journal row in the same
            // transaction. If this throws, the catch below rolls back
            // the UPDATE — phantom-lease (lease held, no journal trail)
            // is impossible by construction.
            try appendWorkJournalEntryLocked(
                episodeId: episodeId,
                generationID: generationID,
                schedulerEpoch: currentEpoch,
                timestamp: now,
                eventType: .acquired,
                cause: nil,
                metadataJSON: metadataJSON
            )

            try exec("COMMIT")
            return true
        } catch {
            // Ignore rollback failures — the primary error is what the
            // caller cares about, and a successful COMMIT cannot be
            // followed by a ROLLBACK that succeeds (so this only fires
            // on the actual failure paths, where it tears down the
            // partial UPDATE).
            try? exec("ROLLBACK")
            throw error
        }
    }

    func releaseLease(jobId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET leaseOwner = NULL, leaseExpiresAt = NULL, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Owner-scoped lease renewal. Returns `true` when the row was
    /// updated (caller still owns the lease) and `false` when no row
    /// matched (lease was reclaimed by orphan recovery, released, or
    /// transferred). Callers MUST check the return value and cancel
    /// their renewal task on `false` — otherwise a renewal task that
    /// outlives its owner could re-seat a NULL-owner row's expiry and
    /// deceive `recoverExpiredLease`.
    func renewLease(jobId: String, owner: String, newExpiresAt: Double) throws -> Bool {
        let sql = """
            UPDATE analysis_jobs
            SET leaseExpiresAt = ?, updatedAt = ?
            WHERE jobId = ? AND leaseOwner = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, newExpiresAt)
        bind(stmt, 2, Date().timeIntervalSince1970)
        bind(stmt, 3, jobId)
        bind(stmt, 4, owner)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
    }

    func fetchJobsByState(_ state: String) throws -> [AnalysisJob] {
        let sql = "SELECT * FROM analysis_jobs WHERE state = ? ORDER BY priority DESC, createdAt ASC"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, state)
        var results: [AnalysisJob] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readJob(stmt))
        }
        return results
    }

    func fetchJobsWithExpiredLeases(before: TimeInterval) throws -> [AnalysisJob] {
        let sql = "SELECT * FROM analysis_jobs WHERE leaseOwner IS NOT NULL AND leaseExpiresAt < ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, before)
        var results: [AnalysisJob] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readJob(stmt))
        }
        return results
    }

    /// playhead-btwk: returns rows that are in an active analysis-jobs state
    /// (`running`, `paused`, `backfill`) and whose `schedulerEpoch` predates
    /// the caller's `currentEpoch`, with no live lease (the lease slot is
    /// either NULL or already expired).
    ///
    /// Stranding shape: a fresh build replaces an older one mid-flight. The
    /// prior process's rows survive at `state='running'` (or `paused`, or —
    /// defensively — `backfill`) but their owner is dead. `recoverExpiredLeases`
    /// only catches rows whose lease is still set-but-expired; cleanly-paused
    /// rows have no lease at all and slip past it. `fetchNextEligibleJob` only
    /// dispatches `queued`/`paused`/`failed`, so a `running` row stays
    /// invisible. This fetch backs the additive sweep that flips them to
    /// `queued` so the scheduler can reclaim the work.
    ///
    /// Live in-flight rows in the current session are excluded by two clauses:
    ///   1. `schedulerEpoch < ?` — the current session's lease acquisitions
    ///      stamp the latest `_meta.scheduler_epoch` value, so an active row
    ///      from this process always equals (never less than) `currentEpoch`.
    ///   2. `(leaseOwner IS NULL OR leaseExpiresAt < ?)` — a live worker
    ///      holds an unexpired lease; the row is its and the sweep cannot yank
    ///      it. Rows whose lease has expired are recovered (the sweep is
    ///      additive against `recoverExpiredLeases`, never destructive).
    ///
    /// `state='backfill'` is included defensively. The current `analysis_jobs`
    /// state machine never writes that value (it is a `SessionState` on
    /// `analysis_assets`), but the bead's spec lists it explicitly so any
    /// future writer of that state — or a hand-edited DB — does not strand
    /// the row.
    func fetchStrandedActiveJobs(now: TimeInterval, currentEpoch: Int) throws -> [AnalysisJob] {
        let sql = """
            SELECT * FROM analysis_jobs
            WHERE state IN ('running', 'paused', 'backfill')
              AND schedulerEpoch < ?
              AND (leaseOwner IS NULL OR leaseExpiresAt < ?)
            ORDER BY priority DESC, createdAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, currentEpoch)
        bind(stmt, 2, now)
        var results: [AnalysisJob] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(readJob(stmt))
        }
        return results
    }

    /// playhead-btwk: flips a stranded row to `queued`, clears any residual
    /// lease, and stamps the row with the caller-supplied epoch so a later
    /// sweep does not see it as stranded again.
    ///
    /// Coverage fields (`featureCoverageSec`, `transcriptCoverageSec`,
    /// `cueCoverageSec`) and `attemptCount` are intentionally untouched —
    /// the sweep is meant to *resume* progress from the prior session, not
    /// restart from zero or penalize the row for an outage that wasn't its
    /// fault. `lastErrorCode` is cleared because any error code attached to
    /// the prior session is no longer informative for the new run.
    /// `nextEligibleAt` is cleared so the row is immediately dispatchable —
    /// any backoff window that may have been set by the prior session is
    /// stale by the time the row is being recovered (the prior process is
    /// gone), and leaving a future `nextEligibleAt` in place would defeat
    /// the entire point of this recovery (the row would stay invisible to
    /// the dispatcher until the timer expired, exactly the symptom this
    /// sweep exists to fix).
    func recoverStrandedActiveJob(
        jobId: String,
        newSchedulerEpoch: Int,
        now: Double
    ) throws {
        let sql = """
            UPDATE analysis_jobs
            SET state = 'queued',
                leaseOwner = NULL,
                leaseExpiresAt = NULL,
                lastErrorCode = NULL,
                nextEligibleAt = NULL,
                schedulerEpoch = ?,
                updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, newSchedulerEpoch)
        bind(stmt, 2, now)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func deleteOldJobs(olderThan: TimeInterval, inStates: [String]) throws -> Int {
        guard !inStates.isEmpty else { return 0 }
        let placeholders = inStates.map { _ in "?" }.joined(separator: ", ")
        let sql = "DELETE FROM analysis_jobs WHERE updatedAt < ? AND state IN (\(placeholders))"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, olderThan)
        for (i, state) in inStates.enumerated() {
            bind(stmt, Int32(i + 2), state)
        }
        try step(stmt, expecting: SQLITE_DONE)
        return Int(sqlite3_changes(db))
    }

    func fetchAllJobEpisodeIds() throws -> Set<String> {
        let sql = "SELECT DISTINCT episodeId FROM analysis_jobs"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var ids = Set<String>()
        while sqlite3_step(stmt) == SQLITE_ROW {
            ids.insert(text(stmt, 0))
        }
        return ids
    }

    /// Fetches episode IDs that have at least one active (non-terminal) job.
    func fetchActiveJobEpisodeIds() throws -> Set<String> {
        let sql = "SELECT DISTINCT episodeId FROM analysis_jobs WHERE state NOT IN ('complete', 'superseded')"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var ids = Set<String>()
        while sqlite3_step(stmt) == SQLITE_ROW {
            ids.insert(text(stmt, 0))
        }
        return ids
    }

    /// Recovers an expired lease: sets state to queued, clears lease fields,
    /// and increments attemptCount.
    func recoverExpiredLease(jobId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET state = 'queued', leaseOwner = NULL, leaseExpiresAt = NULL,
                attemptCount = attemptCount + 1, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Increments the attempt count for a job. Used after failures to drive exponential backoff.
    func incrementAttemptCount(jobId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET attemptCount = attemptCount + 1, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Resets a failed job back to queued state, clearing the error and backoff.
    /// Used by reconciliation when a previously-failed episode's download is still present.
    func resetFailedJobToQueued(jobId: String) throws {
        let sql = """
            UPDATE analysis_jobs
            SET state = 'queued', nextEligibleAt = NULL, lastErrorCode = NULL, updatedAt = ?
            WHERE jobId = ? AND state = 'failed'
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, Date().timeIntervalSince1970)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Batch-updates the state (and optionally nextEligibleAt) for multiple jobs.
    func batchUpdateJobState(jobIds: [String], state: String, nextEligibleAt: Double? = nil) throws {
        guard !jobIds.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            let now = Date().timeIntervalSince1970
            let sql = """
                UPDATE analysis_jobs
                SET state = ?, nextEligibleAt = ?, updatedAt = ?
                WHERE jobId = ?
                """
            let stmt = try prepare(sql)
            defer { sqlite3_finalize(stmt) }
            for jobId in jobIds {
                sqlite3_reset(stmt)
                bind(stmt, 1, state)
                bind(stmt, 2, nextEligibleAt)
                bind(stmt, 3, now)
                bind(stmt, 4, jobId)
                try step(stmt, expecting: SQLITE_DONE)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    // MARK: - CRUD: scheduler_epoch (playhead-uzdq)

    /// Reads the current global scheduler epoch. Returns `nil` when the
    /// row does not exist — all production paths call
    /// `seedSchedulerEpochIfNeeded` during migration, so a `nil` return
    /// indicates a test fixture that skipped seeding.
    func fetchSchedulerEpoch() throws -> Int? {
        let stmt = try prepare("SELECT value FROM _meta WHERE key = 'scheduler_epoch'")
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard let raw = optionalText(stmt, 0), let epoch = Int(raw) else {
            return nil
        }
        return epoch
    }

    /// Atomically increments `_meta.scheduler_epoch` and returns the
    /// new value. The read-modify-write runs inside a BEGIN IMMEDIATE
    /// transaction so a concurrent incrementer cannot produce a
    /// duplicate epoch.
    ///
    /// Caller owns the *scheduling pass* envelope: this method and any
    /// `analysis_jobs` / `work_journal` mutations that belong to the
    /// same scheduling trigger should share a single outer transaction
    /// (see `runSchedulingPass`). When called outside a transaction
    /// this method opens and commits one of its own.
    @discardableResult
    func incrementSchedulerEpoch() throws -> Int {
        let inTxn = sqlite3_get_autocommit(db) == 0
        if !inTxn {
            try exec("BEGIN IMMEDIATE")
        }
        do {
            let current = try fetchSchedulerEpoch() ?? 0
            let next = current + 1
            let upsert = try prepare("""
                INSERT INTO _meta (key, value) VALUES ('scheduler_epoch', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """)
            defer { sqlite3_finalize(upsert) }
            bind(upsert, 1, String(next))
            try step(upsert, expecting: SQLITE_DONE)
            if !inTxn {
                try exec("COMMIT")
            }
            return next
        } catch {
            if !inTxn {
                try? exec("ROLLBACK")
            }
            throw error
        }
    }

    // MARK: - Generic _meta key/value (playhead-gyvb.2)

    /// Read a free-form `_meta` value by key. Returns `nil` if the row is
    /// absent. Used by one-shot launch-time backfill markers (e.g.
    /// `did_duration_backfill_v1`) so feature flags don't have to live
    /// in the schema-version ladder.
    func fetchMetaValue(forKey key: String) throws -> String? {
        let stmt = try prepare("SELECT value FROM _meta WHERE key = ?")
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, key)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return optionalText(stmt, 0)
    }

    /// Upsert a free-form `_meta` value. Counterpart to
    /// `fetchMetaValue(forKey:)`. Treats `_meta` as a key/value store
    /// for one-shot install-time markers.
    func setMetaValue(forKey key: String, value: String) throws {
        let stmt = try prepare("""
            INSERT INTO _meta (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, key)
        bind(stmt, 2, value)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Executes `body` inside a BEGIN IMMEDIATE transaction so every
    /// `analysis_jobs` update, `work_journal` append, and optional
    /// `_meta.scheduler_epoch` bump that belongs to a single scheduling
    /// trigger lands atomically. Matches the bead's "atomic
    /// SchedulingPass" contract.
    ///
    /// The closure runs on the store's actor executor (it is not
    /// `@Sendable`, so it inherits this actor's isolation). Callers
    /// may invoke other `AnalysisStore` methods inside `body`;
    /// nested calls detect the already-open transaction via
    /// `sqlite3_get_autocommit` and reuse the outer envelope.
    func runSchedulingPass<T>(_ body: () throws -> T) throws -> T {
        try exec("BEGIN IMMEDIATE")
        do {
            let result = try body()
            try exec("COMMIT")
            return result
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    // MARK: - playhead-5uvz.3 (Gap-3): processJob outcome-arm atomic commit

    /// Spec describing every write a single `AnalysisWorkScheduler.processJob`
    /// outcome arm performs. Bundling them into a value lets the
    /// scheduler hand the entire arm to the store as one transactional
    /// unit instead of issuing 2-4 separate `await store.X(...)` calls.
    /// Without this, a process kill between two of those awaits could
    /// leave the row at `state='running'` with progress recorded but no
    /// terminal mark — exactly the Gap-3 failure mode the bead audit
    /// flagged at `AnalysisWorkScheduler.swift:1664-1869`.
    ///
    /// Field order in the struct matches the apply order inside
    /// `commitProcessJobOutcomeArm` so callers can read it as a script.
    struct ProcessJobOutcomeArmCommit {
        var jobId: String
        /// If non-nil, runs `updateJobProgress` first (the same call the
        /// scheduler made unconditionally before the switch).
        var progress: ProgressUpdate?
        /// If `true`, runs `incrementAttemptCount` after progress and
        /// before the optional state update. The scheduler precomputes
        /// `attemptCount + 1` (it owns the lease, so no concurrent
        /// writer) instead of round-tripping a fetch.
        var incrementAttempt: Bool = false
        /// If non-nil, runs `insertJob` for the next-tier child row
        /// (only used by the tier-advance arm).
        var insertNextJob: AnalysisJob?
        /// If non-nil, runs `updateJobState` (the terminal/transition
        /// write).
        var stateUpdate: StateUpdate?
        /// If `true` (default), runs `releaseLease` as the last write
        /// inside the transaction — closes the Gap-3 window where a
        /// crash between the terminal mark and the lease release
        /// stranded the row.
        var releaseLease: Bool = true

        struct ProgressUpdate: Equatable {
            var featureCoverageSec: Double
            var transcriptCoverageSec: Double
            var cueCoverageSec: Double
        }

        struct StateUpdate: Equatable {
            var state: String
            var nextEligibleAt: Double?
            var lastErrorCode: String?
        }
    }

    /// Applies a `ProcessJobOutcomeArmCommit` inside a single
    /// `BEGIN IMMEDIATE..COMMIT` transaction via `runSchedulingPass`.
    /// If any step throws (real SQLite error or DEBUG fault injection),
    /// the entire arm rolls back — progress, increment, child insert,
    /// state update, and lease release commit or are reverted as one
    /// unit.
    ///
    /// Mirrors the atomicity contract pattern from
    /// `acquireLeaseWithJournal`: the production scheduler relies on
    /// "no torn arm" rather than auditing each transition manually.
    func commitProcessJobOutcomeArm(_ commit: ProcessJobOutcomeArmCommit) throws {
        try runSchedulingPass {
            if let progress = commit.progress {
                try updateJobProgress(
                    jobId: commit.jobId,
                    featureCoverageSec: progress.featureCoverageSec,
                    transcriptCoverageSec: progress.transcriptCoverageSec,
                    cueCoverageSec: progress.cueCoverageSec
                )
            }

            #if DEBUG
            try triggerProcessJobOutcomeFaultIfNeeded(.afterProgressUpdateBeforeStateUpdate)
            #endif

            if commit.incrementAttempt {
                try incrementAttemptCount(jobId: commit.jobId)
            }

            if let nextJob = commit.insertNextJob {
                _ = try insertJob(nextJob)
            }

            if let stateUpdate = commit.stateUpdate {
                try updateJobState(
                    jobId: commit.jobId,
                    state: stateUpdate.state,
                    nextEligibleAt: stateUpdate.nextEligibleAt,
                    lastErrorCode: stateUpdate.lastErrorCode
                )
            }

            #if DEBUG
            try triggerProcessJobOutcomeFaultIfNeeded(.afterStateUpdateBeforeLeaseRelease)
            #endif

            if commit.releaseLease {
                try releaseLease(jobId: commit.jobId)
            }
        }
    }

    #if DEBUG
    /// Test seam for crash-rollback verification (playhead-uzdq).
    /// Runs a scheduling pass that bumps the scheduler epoch, appends
    /// a `work_journal` row, and then throws — all inside the outer
    /// `runSchedulingPass` transaction so both writes roll back.
    ///
    /// This is exposed as an actor-isolated method (rather than
    /// asking tests to hand-roll a `@Sendable` closure) so the test
    /// file does not have to reason about closure isolation.
    func simulateCrashInSchedulingPassForTesting(
        episodeId: String,
        generationID: UUID,
        timestamp: Double
    ) throws {
        try runSchedulingPass {
            _ = try incrementSchedulerEpoch()
            let entry = WorkJournalEntry(
                id: UUID().uuidString,
                episodeId: episodeId,
                generationID: generationID,
                schedulerEpoch: 99_999,
                timestamp: timestamp,
                eventType: .acquired,
                cause: nil,
                metadata: "{}",
                artifactClass: .scratch
            )
            try appendWorkJournalEntry(entry)
            throw CrashRollbackTestError.simulated
        }
    }

    /// Sentinel error thrown by ``simulateCrashInSchedulingPassForTesting`` so
    /// tests can assert the transactional body rolled back.
    enum CrashRollbackTestError: Error, Equatable {
        case simulated
    }
    #endif

    // MARK: - CRUD: episode execution lease (playhead-uzdq)

    /// Acquires an episode-level lease by taking the first eligible
    /// `analysis_jobs` row for `episodeId` (ORDER BY priority DESC,
    /// createdAt ASC) whose lease slot is free (`leaseOwner IS NULL` or
    /// `leaseExpiresAt < now`) and CAS-setting the lease columns to the
    /// caller's identity.
    ///
    /// Transactional: the eligibility probe, epoch validation, CAS
    /// update, and `acquired` journal append all run inside a single
    /// BEGIN IMMEDIATE..COMMIT envelope.
    ///
    /// Errors:
    /// - `LeaseError.staleEpoch` if `schedulerEpoch` < the store's
    ///   current epoch.
    /// - `LeaseError.noJobForEpisode` if no `analysis_jobs` row exists
    ///   for `episodeId`.
    /// - `LeaseError.leaseHeld` if a live lease already covers every
    ///   row for the episode.
    func acquireEpisodeLease(
        episodeId: String,
        ownerWorkerId: String,
        generationID: String,
        schedulerEpoch: Int,
        now: Double,
        ttlSeconds: Double,
        metadataJSON: String = "{}"
    ) throws -> EpisodeExecutionLeaseDescriptor {
        try exec("BEGIN IMMEDIATE")
        do {
            // Epoch validation first: a stale epoch short-circuits
            // before we touch any state. Fresh databases that somehow
            // escaped migration have no epoch row; treat that as
            // epoch=0 and allow the acquisition.
            let currentEpoch = try fetchSchedulerEpoch() ?? 0
            if schedulerEpoch < currentEpoch {
                try exec("ROLLBACK")
                throw LeaseError.staleEpoch(expected: schedulerEpoch, actual: currentEpoch)
            }

            // Find the best candidate row for this episode whose lease
            // slot is free. ORDER matches the spec: higher priority
            // first, then oldest (FIFO within priority).
            let findSQL = """
                SELECT jobId FROM analysis_jobs
                WHERE episodeId = ?
                  AND (leaseOwner IS NULL OR leaseExpiresAt < ?)
                ORDER BY priority DESC, createdAt ASC, jobId ASC
                LIMIT 1
                """
            let findStmt = try prepare(findSQL)
            bind(findStmt, 1, episodeId)
            bind(findStmt, 2, now)
            let stepRc = sqlite3_step(findStmt)
            defer { sqlite3_finalize(findStmt) }
            if stepRc != SQLITE_ROW {
                // Distinguish "no row at all" from "all rows held".
                let existsStmt = try prepare("SELECT 1 FROM analysis_jobs WHERE episodeId = ? LIMIT 1")
                defer { sqlite3_finalize(existsStmt) }
                bind(existsStmt, 1, episodeId)
                let hasRow = sqlite3_step(existsStmt) == SQLITE_ROW
                try exec("ROLLBACK")
                if hasRow {
                    throw LeaseError.leaseHeld(episodeId: episodeId)
                } else {
                    throw LeaseError.noJobForEpisode(episodeId: episodeId)
                }
            }
            let jobId = text(findStmt, 0)

            // CAS: the eligibility probe above can race under WAL if a
            // competitor opened its own BEGIN IMMEDIATE and committed
            // between our probe and our update. Guard the update with
            // the same `leaseOwner IS NULL OR leaseExpiresAt < ?`
            // predicate; zero rows changed means we lost the race.
            let expiresAt = now + ttlSeconds
            let updateSQL = """
                UPDATE analysis_jobs
                SET leaseOwner = ?, leaseExpiresAt = ?,
                    generationID = ?, schedulerEpoch = ?,
                    state = CASE WHEN state IN ('complete','superseded') THEN state ELSE 'running' END,
                    updatedAt = ?
                WHERE jobId = ? AND (leaseOwner IS NULL OR leaseExpiresAt < ?)
                """
            let updateStmt = try prepare(updateSQL)
            defer { sqlite3_finalize(updateStmt) }
            bind(updateStmt, 1, ownerWorkerId)
            bind(updateStmt, 2, expiresAt)
            bind(updateStmt, 3, generationID)
            bind(updateStmt, 4, schedulerEpoch)
            bind(updateStmt, 5, now)
            bind(updateStmt, 6, jobId)
            bind(updateStmt, 7, now)
            try step(updateStmt, expecting: SQLITE_DONE)
            if sqlite3_changes(db) == 0 {
                try exec("ROLLBACK")
                throw LeaseError.leaseHeld(episodeId: episodeId)
            }

            // Append the `acquired` journal row in the same txn.
            try appendWorkJournalEntryLocked(
                episodeId: episodeId,
                generationID: generationID,
                schedulerEpoch: schedulerEpoch,
                timestamp: now,
                eventType: .acquired,
                cause: nil,
                metadataJSON: metadataJSON
            )

            try exec("COMMIT")
            return EpisodeExecutionLeaseDescriptor(
                jobId: jobId,
                episodeId: episodeId,
                ownerWorkerId: ownerWorkerId,
                generationID: generationID,
                schedulerEpoch: schedulerEpoch,
                acquiredAt: now,
                expiresAt: expiresAt
            )
        } catch {
            // Ignore rollback failures — the primary error is already
            // the thing the caller cares about.
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Extends the lease TTL iff the caller's {generationID,
    /// schedulerEpoch} still match the row. Stale callers get
    /// `LeaseError.staleEpoch` or `.generationMismatch` and their write
    /// is rejected.
    func renewEpisodeLease(
        episodeId: String,
        generationID: String,
        schedulerEpoch: Int,
        newExpiresAt: Double,
        now: Double
    ) throws {
        try exec("BEGIN IMMEDIATE")
        do {
            let currentEpoch = try fetchSchedulerEpoch() ?? 0
            if schedulerEpoch < currentEpoch {
                try exec("ROLLBACK")
                throw LeaseError.staleEpoch(expected: schedulerEpoch, actual: currentEpoch)
            }
            // Guard against zombie resurrection: if our own lease has
            // already expired we must NOT extend it here. A live lease
            // (`leaseExpiresAt >= now`) is still ours to renew; an
            // expired one means acquire-time eligibility has to run
            // again. Without this check, a worker that slept past its
            // TTL would silently reclaim a row that another acquirer
            // could legitimately take.
            let sql = """
                UPDATE analysis_jobs
                SET leaseExpiresAt = ?, updatedAt = ?
                WHERE episodeId = ? AND generationID = ? AND schedulerEpoch = ?
                  AND leaseExpiresAt IS NOT NULL AND leaseExpiresAt >= ?
                """
            let stmt = try prepare(sql)
            defer { sqlite3_finalize(stmt) }
            bind(stmt, 1, newExpiresAt)
            bind(stmt, 2, now)
            bind(stmt, 3, episodeId)
            bind(stmt, 4, generationID)
            bind(stmt, 5, schedulerEpoch)
            bind(stmt, 6, now)
            try step(stmt, expecting: SQLITE_DONE)
            if sqlite3_changes(db) == 0 {
                try exec("ROLLBACK")
                throw LeaseError.generationMismatch(episodeId: episodeId)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Releases an episode lease and records the terminal event in the
    /// WorkJournal. Idempotent for terminal events (`finalized`,
    /// `failed`): if a matching terminal row already exists for
    /// {episodeId, generationID}, the second call is a no-op and does
    /// NOT append a duplicate journal row.
    ///
    /// When the caller's {generationID} does not match the row (e.g.
    /// late callback after a requeue), `LeaseError.generationMismatch`
    /// is thrown.
    func releaseEpisodeLease(
        episodeId: String,
        generationID: String,
        schedulerEpoch: Int,
        eventType: WorkJournalEntry.EventType,
        cause: InternalMissCause?,
        now: Double,
        metadataJSON: String = "{}"
    ) throws {
        try exec("BEGIN IMMEDIATE")
        do {
            // Idempotence guard for terminal events: if a matching
            // terminal journal row already exists, treat this call as
            // a duplicate and return early. We check BEFORE the lease
            // clear so a second `finalized` against an already-released
            // row doesn't log a second row OR raise generationMismatch.
            if eventType == .finalized || eventType == .failed {
                if try terminalJournalRowExists(
                    episodeId: episodeId,
                    generationID: generationID,
                    eventType: eventType
                ) {
                    try exec("COMMIT")
                    return
                }
            }

            // Clear lease fields on the matching row (if any). We do
            // NOT gate on schedulerEpoch here — a release after an
            // epoch bump is still the legitimate owner tearing down.
            let updateSQL = """
                UPDATE analysis_jobs
                SET leaseOwner = NULL, leaseExpiresAt = NULL, updatedAt = ?
                WHERE episodeId = ? AND generationID = ?
                """
            let updateStmt = try prepare(updateSQL)
            defer { sqlite3_finalize(updateStmt) }
            bind(updateStmt, 1, now)
            bind(updateStmt, 2, episodeId)
            bind(updateStmt, 3, generationID)
            try step(updateStmt, expecting: SQLITE_DONE)
            if sqlite3_changes(db) == 0 {
                try exec("ROLLBACK")
                throw LeaseError.generationMismatch(episodeId: episodeId)
            }

            try appendWorkJournalEntryLocked(
                episodeId: episodeId,
                generationID: generationID,
                schedulerEpoch: schedulerEpoch,
                timestamp: now,
                eventType: eventType,
                cause: cause,
                metadataJSON: metadataJSON
            )
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Returns every `analysis_jobs` row whose lease slot is held but
    /// has expired more than `graceSeconds` before `now`. Used on cold
    /// launch by `AnalysisCoordinator.recoverOrphans`.
    ///
    /// The grace window matches the bead's "now - 10s" filter so we
    /// don't race a currently-executing lease whose next renewal is
    /// about to land.
    func fetchEpisodesWithExpiredLeases(
        now: Double,
        graceSeconds: Double = 10
    ) throws -> [AnalysisJob] {
        let sql = """
            SELECT * FROM analysis_jobs
            WHERE leaseOwner IS NOT NULL AND leaseExpiresAt < ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, now - graceSeconds)
        var out: [AnalysisJob] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            out.append(readJob(stmt))
        }
        return out
    }

    /// Fetches the most-recent WorkJournal `cause` for `episodeId`,
    /// across ALL generations. Returns `nil` when no entry exists or the
    /// most-recent entry has a NULL cause column.
    ///
    /// Used by the batch-notification summary builder (playhead-0a0s) to
    /// derive a per-episode `InternalMissCause` for the surface-status
    /// reducer without taking on full WorkJournal-row deserialization.
    /// The query intentionally collapses across generations because the
    /// batch path only cares about the latest blocker the episode
    /// experienced — not which scheduler-epoch it belongs to.
    func fetchLastWorkJournalCause(episodeId: String) throws -> InternalMissCause? {
        let sql = """
            SELECT cause
            FROM work_journal
            WHERE episode_id = ? AND cause IS NOT NULL
            ORDER BY timestamp DESC, rowid DESC
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard let raw = optionalText(stmt, 0) else { return nil }
        return InternalMissCause(rawValue: raw) ?? .unknown(raw)
    }

    /// Fetches the most-recent WorkJournal entry for the given
    /// {episodeId, generationID}. Returns `nil` when no entry exists.
    func fetchLastWorkJournalEntry(
        episodeId: String,
        generationID: String
    ) throws -> WorkJournalEntry? {
        let sql = """
            SELECT id, episode_id, generation_id, scheduler_epoch,
                   timestamp, event_type, cause, metadata, artifact_class
            FROM work_journal
            WHERE episode_id = ? AND generation_id = ?
            ORDER BY timestamp DESC, rowid DESC
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        bind(stmt, 2, generationID)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readWorkJournalEntry(stmt)
    }

    /// Fetches every WorkJournal row for a {episodeId, generationID}
    /// pair, ordered oldest-first. Primarily for testing / audit; the
    /// happy-path `fetchLastWorkJournalEntry` is what orphan recovery
    /// uses in production.
    func fetchWorkJournalEntries(
        episodeId: String,
        generationID: String
    ) throws -> [WorkJournalEntry] {
        let sql = """
            SELECT id, episode_id, generation_id, scheduler_epoch,
                   timestamp, event_type, cause, metadata, artifact_class
            FROM work_journal
            WHERE episode_id = ? AND generation_id = ?
            ORDER BY timestamp ASC, rowid ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        bind(stmt, 2, generationID)
        var out: [WorkJournalEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            out.append(try readWorkJournalEntry(stmt))
        }
        return out
    }

    /// Fetches the most-recent `limit` WorkJournal entries across ALL
    /// episodes and generations, ordered by `(timestamp DESC, rowid DESC)`
    /// so the absolute-newest row is `out[0]`. Used by the support-safe
    /// diagnostics bundle (playhead-ghon) to populate both
    /// `scheduler_events` (last 200) and `work_journal_tail` (last 50).
    ///
    /// The two diagnostics consumers use different orderings — scheduler
    /// events are emitted newest-first, the journal tail is emitted in
    /// insertion order — so this method returns the rawest possible
    /// "newest N rows" set and the builder re-orders client-side. That
    /// keeps SQL simple and the projection rules pure.
    func fetchRecentWorkJournalEntries(limit: Int) throws -> [WorkJournalEntry] {
        precondition(limit >= 0, "fetchRecentWorkJournalEntries: limit must be non-negative")
        if limit == 0 { return [] }
        let sql = """
            SELECT id, episode_id, generation_id, scheduler_epoch,
                   timestamp, event_type, cause, metadata, artifact_class
            FROM work_journal
            ORDER BY timestamp DESC, rowid DESC
            LIMIT ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, limit)
        var out: [WorkJournalEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            out.append(try readWorkJournalEntry(stmt))
        }
        return out
    }

    /// Requeue an orphaned analysis_jobs row: clear the stale lease,
    /// assign a fresh generationID + new scheduler epoch, reset
    /// attemptCount to 0 and apply the (possibly demoted) priority.
    /// All mutations run inside a single transaction so recovery is
    /// crash-consistent.
    ///
    /// Lane preservation: caller computes the new priority per bead
    /// policy (same band except Now > 60s stale → demote to Soon).
    /// This method does NOT recompute lanes — it takes the new priority
    /// verbatim.
    func requeueOrphanedLease(
        jobId: String,
        newGenerationID: String,
        newSchedulerEpoch: Int,
        newPriority: Int,
        now: Double
    ) throws {
        let sql = """
            UPDATE analysis_jobs
            SET leaseOwner = NULL, leaseExpiresAt = NULL,
                generationID = ?, schedulerEpoch = ?,
                priority = ?, attemptCount = 0,
                state = CASE WHEN state = 'running' THEN 'queued' ELSE state END,
                updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, newGenerationID)
        bind(stmt, 2, newSchedulerEpoch)
        bind(stmt, 3, newPriority)
        bind(stmt, 4, now)
        bind(stmt, 5, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Clears the lease slot on a row that has reached a terminal event
    /// (finalized / failed) but was stranded on cold launch. Does NOT
    /// requeue.
    func clearOrphanedLeaseNoRequeue(jobId: String, now: Double) throws {
        let sql = """
            UPDATE analysis_jobs
            SET leaseOwner = NULL, leaseExpiresAt = NULL, updatedAt = ?
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, now)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Appends a free-standing WorkJournal entry. Most call-sites rely
    /// on the implicit appends from `acquireEpisodeLease` /
    /// `releaseEpisodeLease`; this method is exposed for
    /// `checkpointed` events (which do not transition the lease slot)
    /// and for test harnesses that seed specific journal shapes.
    func appendWorkJournalEntry(_ entry: WorkJournalEntry) throws {
        let inTxn = sqlite3_get_autocommit(db) == 0
        if !inTxn {
            try exec("BEGIN IMMEDIATE")
        }
        do {
            try insertWorkJournalRow(entry)
            if !inTxn {
                try exec("COMMIT")
            }
        } catch {
            if !inTxn {
                try? exec("ROLLBACK")
            }
            throw error
        }
    }

    /// Internal append that assumes an outer transaction is already
    /// open. Used by `acquireEpisodeLease` and `releaseEpisodeLease`.
    private func appendWorkJournalEntryLocked(
        episodeId: String,
        generationID: String,
        schedulerEpoch: Int,
        timestamp: Double,
        eventType: WorkJournalEntry.EventType,
        cause: InternalMissCause?,
        metadataJSON: String
    ) throws {
        let entry = WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: UUID(uuidString: generationID) ?? UUID(),
            schedulerEpoch: schedulerEpoch,
            timestamp: timestamp,
            eventType: eventType,
            cause: cause,
            metadata: metadataJSON,
            artifactClass: .scratch
        )
        // The entry we just built has a fresh id; we insert using the
        // passed-in generationID raw string, NOT the UUID we parsed,
        // so the CAS/orphan-recovery joins still match rows whose
        // generation was stored as a non-canonical string.
        try insertWorkJournalRow(entry, rawGenerationID: generationID)
    }

    /// Private row INSERT. `rawGenerationID` lets callers persist the
    /// exact string they were handed (the coordinator always passes a
    /// canonical UUID, but tests may seed arbitrary strings).
    private func insertWorkJournalRow(
        _ entry: WorkJournalEntry,
        rawGenerationID: String? = nil
    ) throws {
        let sql = """
            INSERT INTO work_journal
            (id, episode_id, generation_id, scheduler_epoch, timestamp,
             event_type, cause, metadata, artifact_class)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, entry.id)
        bind(stmt, 2, entry.episodeId)
        bind(stmt, 3, rawGenerationID ?? entry.generationID.uuidString)
        bind(stmt, 4, entry.schedulerEpoch)
        bind(stmt, 5, entry.timestamp)
        bind(stmt, 6, entry.eventType.rawValue)
        bind(stmt, 7, entry.cause?.rawValue)
        bind(stmt, 8, entry.metadata)
        bind(stmt, 9, entry.artifactClass.rawValue)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Idempotence probe used by `releaseEpisodeLease` for terminal
    /// events. Returns `true` iff a matching row already exists.
    private func terminalJournalRowExists(
        episodeId: String,
        generationID: String,
        eventType: WorkJournalEntry.EventType
    ) throws -> Bool {
        let sql = """
            SELECT 1 FROM work_journal
            WHERE episode_id = ? AND generation_id = ? AND event_type = ?
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, episodeId)
        bind(stmt, 2, generationID)
        bind(stmt, 3, eventType.rawValue)
        return sqlite3_step(stmt) == SQLITE_ROW
    }

    private func readWorkJournalEntry(_ stmt: OpaquePointer?) throws -> WorkJournalEntry {
        let eventRaw = try requireText(stmt, 5)
        guard let eventType = WorkJournalEntry.EventType(rawValue: eventRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown work_journal event_type '\(eventRaw)'")
        }
        // `cause` is nullable at the column level. playhead-uzdq.1 Fix #4:
        // on an unknown rawValue we now promote to the forward-compat
        // `.unknown(rawCause)` sentinel instead of silently downgrading to
        // `.pipelineError`, which would have poisoned cause-taxonomy
        // telemetry whenever the enum evolves.
        let cause: InternalMissCause?
        if let rawCause = optionalText(stmt, 6) {
            cause = InternalMissCause(rawValue: rawCause) ?? .unknown(rawCause)
        } else {
            cause = nil
        }
        // playhead-uzdq.1 Fix #3: a non-UUID `generation_id` is corruption
        // — every writer persists `UUID.uuidString`, and silently
        // substituting a fresh UUID would hide the corruption AND break
        // the `{episode_id, generation_id}` identity that orphan recovery
        // joins on. Throw instead of papering over.
        let generationRaw = try requireText(stmt, 2)
        guard let generationUUID = UUID(uuidString: generationRaw) else {
            throw AnalysisStoreError.queryFailed(
                "Non-UUID generation_id in work_journal: '\(generationRaw)'"
            )
        }
        let artifactRaw = optionalText(stmt, 8) ?? ArtifactClass.scratch.rawValue
        let artifactClass = ArtifactClass(rawValue: artifactRaw) ?? .scratch
        return WorkJournalEntry(
            id: try requireText(stmt, 0),
            episodeId: try requireText(stmt, 1),
            generationID: generationUUID,
            schedulerEpoch: Int(sqlite3_column_int(stmt, 3)),
            timestamp: sqlite3_column_double(stmt, 4),
            eventType: eventType,
            cause: cause,
            metadata: optionalText(stmt, 7) ?? "{}",
            artifactClass: artifactClass
        )
    }

    // MARK: - CRUD: backfill_jobs

    /// Inserts a new backfill job. Throws `AnalysisStoreError.duplicateJobId`
    /// if the row already exists — callers must explicitly choose between
    /// insert-new and update-existing semantics (H7).
    func insertBackfillJob(_ job: BackfillJob) throws {
        let sql = """
            INSERT INTO backfill_jobs
            (jobId, analysisAssetId, podcastId, phase, coveragePolicy, priority,
             progressCursor, retryCount, deferReason, status, scanCohortJSON,
             createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, job.jobId)
        bind(stmt, 2, job.analysisAssetId)
        bind(stmt, 3, job.podcastId)
        bind(stmt, 4, job.phase.rawValue)
        bind(stmt, 5, job.coveragePolicy.rawValue)
        bind(stmt, 6, job.priority)
        bind(stmt, 7, try encodeJSONString(job.progressCursor))
        bind(stmt, 8, job.retryCount)
        bind(stmt, 9, job.deferReason)
        bind(stmt, 10, job.status.rawValue)
        bind(stmt, 11, job.scanCohortJSON)
        bind(stmt, 12, job.createdAt)
        do {
            try step(stmt, expecting: SQLITE_DONE)
        } catch {
            // SQLite constraint errors come back as the primary code
            // SQLITE_CONSTRAINT (19); the extended subcodes are stable across
            // versions but are not exported as Swift symbols by the SQLite3
            // module. We hand-roll the literals here.
            //   SQLITE_CONSTRAINT_PRIMARYKEY = 19 | (6<<8) = 1555
            //   SQLITE_CONSTRAINT_UNIQUE     = 19 | (8<<8) = 2067
            let extended = sqlite3_extended_errcode(db)
            if extended == 1555 || extended == 2067 {
                throw AnalysisStoreError.duplicateJobId(job.jobId)
            }
            throw error
        }
    }

    func fetchBackfillJob(byId jobId: String) throws -> BackfillJob? {
        // Column order: jobId, analysisAssetId, podcastId, phase, coveragePolicy,
        // priority, progressCursor, retryCount, deferReason, status,
        // scanCohortJSON, createdAt.
        let sql = """
            SELECT jobId, analysisAssetId, podcastId, phase, coveragePolicy,
                   priority, progressCursor, retryCount, deferReason, status,
                   scanCohortJSON, createdAt
            FROM backfill_jobs WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readBackfillJob(stmt)
    }

    /// H5: progress-only checkpoint. Writes `progressCursor`, `retryCount`,
    /// and bumps no other fields. Use this for periodic in-flight progress
    /// updates so a concurrent or earlier `markBackfillJobDeferred` call is
    /// not silently overwritten.
    func checkpointBackfillJobProgress(
        jobId: String,
        progressCursor: BackfillProgressCursor?,
        retryCount: Int? = nil
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET progressCursor = ?, retryCount = COALESCE(?, retryCount)
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, try encodeJSONString(progressCursor))
        bind(stmt, 2, retryCount)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// H5: defer a job. Writes `status='deferred'` and the supplied reason
    /// while preserving the existing `progressCursor` so resumption from the
    /// last checkpoint still works.
    ///
    /// C-R3-1: guarded against silent terminal resurrection — the same pattern
    /// as C3-2's guards on `markBackfillJobComplete` / `markBackfillJobFailed`.
    /// The update is restricted to rows in `queued` or `running`; on zero-row
    /// updates we probe the current state and:
    ///   - return silently after refreshing `deferReason` when the row is
    ///     already `deferred` (idempotent retry path — an operator issuing a
    ///     new defer reason expects the row to reflect the most recent cause),
    ///   - throw `invalidStateTransition` on any other state (including a
    ///     missing row or a terminal `.complete` / `.failed` row) so the H-1
    ///     drain loop cannot silently demote a `.failed` row to `.deferred`
    ///     and lose the original failure reason.
    func markBackfillJobDeferred(
        jobId: String,
        reason: String
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'deferred', deferReason = ?
            WHERE jobId = ? AND status IN ('queued', 'running')
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, reason)
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) == 0 {
            let current = try probeBackfillJobStatus(jobId: jobId)
            if current == "deferred" {
                // Already deferred: refresh deferReason so the most recent
                // cause is visible to operators, but leave status untouched.
                let refreshSQL = """
                    UPDATE backfill_jobs
                    SET deferReason = ?
                    WHERE jobId = ? AND status = 'deferred'
                    """
                let refreshStmt = try prepare(refreshSQL)
                defer { sqlite3_finalize(refreshStmt) }
                bind(refreshStmt, 1, reason)
                bind(refreshStmt, 2, jobId)
                try step(refreshStmt, expecting: SQLITE_DONE)
                return
            }
            throw AnalysisStoreError.invalidStateTransition(
                jobId: jobId,
                fromStatus: current,
                toStatus: "deferred"
            )
        }
    }

    /// C-2: transition a job row to `status='running'` without clobbering
    /// `deferReason`, `progressCursor`, or `retryCount`. Preserving the
    /// `deferReason` on a running-after-defer transition keeps the audit
    /// trail intact: the row reflects that an earlier defer happened even
    /// as the next runner attempt starts executing.
    ///
    /// Round-2 fix: the unconditional UPDATE silently resurrected terminal
    /// rows (`.complete` / `.failed`) into `.running`, defeating the whole
    /// point of the H5 split. The status guard limits the transition to
    /// rows in `.queued` or `.deferred`; any other state (including a
    /// missing row) throws `AnalysisStoreError.invalidStateTransition` so
    /// callers learn about the programmer/race error instead of silently
    /// re-running a job that already finished.
    /// HIGH-R6-1: idempotent on an existing `.running` row. The prior
    /// implementation only accepted `queued`/`deferred` and threw
    /// `invalidStateTransition` on an already-running row. That asymmetry
    /// (the Complete/Failed/Deferred guards are all idempotent on their
    /// own terminal state) meant a process crash between
    /// `markBackfillJobRunning` and the subsequent terminal transition
    /// left the row stuck in `.running`. On the next drain the runner
    /// would re-enqueue via M-5 idempotency, call this method, hit the
    /// throw, and the runner's "already terminal" catch arm would
    /// `continue` without bumping `retryCount` — a zombie that loops
    /// forever. The `IN (..., 'running')` clause restores symmetry; the
    /// row is left untouched (no field clobbering) because the UPDATE is
    /// a no-op when the row is already `.running`.
    func markBackfillJobRunning(jobId: String) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'running'
            WHERE jobId = ? AND status IN ('queued', 'deferred', 'running')
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) == 0 {
            // With 'running' included in the IN clause above, a zero-change
            // result means the row is either missing or in a terminal state
            // (`complete`/`failed`). Probe for defensive disambiguation: if
            // the row somehow reports `.running` (e.g. a future schema
            // change widens the set) treat it as idempotent success;
            // anything else is a real invalid transition.
            let current = try probeBackfillJobStatus(jobId: jobId)
            if current == "running" {
                return
            }
            throw AnalysisStoreError.invalidStateTransition(
                jobId: jobId,
                fromStatus: current,
                toStatus: "running"
            )
        }
    }

    /// C-2: terminal success transition. Writes the final `progressCursor`
    /// and flips `status='complete'` while preserving `deferReason` (audit
    /// trail) and `retryCount`.
    ///
    /// C3-2: guarded against silent terminal resurrection. The update is
    /// restricted to rows in `queued`, `deferred`, or `running`; if zero
    /// rows are affected we probe the current state and:
    ///   - return silently when the row is already `complete` (idempotent
    ///     retry after an earlier successful call),
    ///   - throw `invalidStateTransition` on any other state (including a
    ///     missing row or a `failed` row) so callers can never silently
    ///     promote a failed job to complete.
    func markBackfillJobComplete(
        jobId: String,
        progressCursor: BackfillProgressCursor?
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'complete', progressCursor = ?
            WHERE jobId = ? AND status IN ('queued', 'deferred', 'running')
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, try encodeJSONString(progressCursor))
        bind(stmt, 2, jobId)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) == 0 {
            let current = try probeBackfillJobStatus(jobId: jobId)
            if current == "complete" {
                // Already complete: idempotent retry path, silent success.
                return
            }
            throw AnalysisStoreError.invalidStateTransition(
                jobId: jobId,
                fromStatus: current,
                toStatus: "complete"
            )
        }
    }

    /// C-2: terminal failure transition. The prior shim silently dropped
    /// `deferReason` on `.failed`; this method ensures the reason is
    /// written so operators can diagnose why a job failed without scraping
    /// logs.
    ///
    /// M-4: note that this intentionally overwrites any prior
    /// `deferReason`. A job that was previously `.deferred` for thermal
    /// throttling and then failed the next attempt must record the newer
    /// *failure* cause, not the older defer reason. Operators diagnosing a
    /// failed job care about why it failed, not the cooldown that preceded
    /// it; the defer history is still recoverable from structured logs.
    /// This behavior is pinned by
    /// `markBackfillJobFailed_overwritesDeferReason`.
    ///
    /// C3-2: guarded against silent terminal resurrection. The update is
    /// restricted to rows in `queued`, `deferred`, or `running`; if zero
    /// rows are affected we probe the current state and:
    ///   - return silently when the row is already `failed` (idempotent
    ///     retry after an earlier failure was recorded; `retryCount` and
    ///     the original failure `deferReason` are preserved),
    ///   - throw `invalidStateTransition` on any other state (including a
    ///     missing row or a `complete` row) so a late exception cannot
    ///     silently demote a completed job.
    func markBackfillJobFailed(
        jobId: String,
        reason: String,
        retryCount: Int
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = 'failed', deferReason = ?, retryCount = ?
            WHERE jobId = ? AND status IN ('queued', 'deferred', 'running')
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, reason)
        bind(stmt, 2, retryCount)
        bind(stmt, 3, jobId)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) == 0 {
            let current = try probeBackfillJobStatus(jobId: jobId)
            if current == "failed" {
                // Already failed: idempotent retry path, silent success.
                // retryCount and the original deferReason are preserved —
                // the caller's newer values are intentionally discarded so
                // a double-catch at a higher layer cannot double-bump the
                // retry counter.
                return
            }
            throw AnalysisStoreError.invalidStateTransition(
                jobId: jobId,
                fromStatus: current,
                toStatus: "failed"
            )
        }
    }

    /// C3-2: small helper used by the guarded terminal transitions to
    /// distinguish "no row" from "row present in a disallowed state". Returns
    /// `nil` when no row exists for `jobId`.
    private func probeBackfillJobStatus(jobId: String) throws -> String? {
        let stmt = try prepare("SELECT status FROM backfill_jobs WHERE jobId = ? LIMIT 1")
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, jobId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return optionalText(stmt, 0)
    }

    #if DEBUG
    /// bd-m8k test-only helper: drop the `podcast_planner_state` table from
    /// under the live connection so the next `migrate()` against this path
    /// has to recreate it. Used by the "DROP TABLE / re-migrate cycle is
    /// clean" regression test. Production code must never call this — the
    /// table is the planner's source of truth, and dropping it would erase
    /// every show's observed-episode counter and precision ring.
    func dropPodcastPlannerStateForTesting() throws {
        try exec("DROP TABLE IF EXISTS podcast_planner_state")
    }

    /// Cycle 2 Rev4-M3 test-only helper: run an arbitrary DDL/DML
    /// statement so tests can corrupt rows on purpose to exercise the
    /// fetchPodcastPlannerState clamp warning. Production code MUST NOT
    /// call this; it bypasses every validator the store enforces.
    func execForTesting(_ sql: String) throws {
        try exec(sql)
    }

    func setFeatureBatchPersistenceFaultInjectionForTesting(
        _ injection: FeatureBatchPersistenceFaultInjection?
    ) {
        featureBatchPersistenceFaultInjection = injection
    }

    /// playhead-uhdu (5uvz.1 NIT #1): arms a one-shot fault inside
    /// `acquireLeaseWithJournal` so tests can validate the rollback
    /// contract on the journal-append path. Production code MUST NOT
    /// call this — the setter is only present in DEBUG builds.
    func setLeaseJournalFaultInjectionForTesting(
        _ injection: LeaseJournalFaultInjection?
    ) {
        leaseJournalFaultInjection = injection
    }

    /// playhead-5uvz.3 (Gap-3): arms a one-shot fault inside the
    /// `AnalysisWorkScheduler.processJob` outcome-arm transaction so
    /// tests can validate that progress + state + lease release roll
    /// back together if any inner write throws. Production code MUST
    /// NOT call this.
    func setProcessJobOutcomeFaultInjectionForTesting(
        _ injection: ProcessJobOutcomeFaultInjection?
    ) {
        processJobOutcomeFaultInjection = injection
    }
    #endif

    #if DEBUG
    /// Test-only: force a backfill row to a specific state without running
    /// the lifecycle guards. Used by tests that need to set up a pre-existing
    /// row in a specific configuration before exercising the runner (e.g.
    /// demoting a terminal row back to `.queued` to simulate an orphan
    /// recovery scenario). Production code MUST NOT call this — use
    /// `markBackfillJobRunning/Complete/Failed/Deferred` or
    /// `checkpointBackfillJobProgress` instead.
    ///
    /// `progressCursor` is written unconditionally: passing `nil` clears the
    /// column to NULL. `retryCount` and `deferReason` use COALESCE so nil
    /// leaves the existing row values untouched.
    func forceBackfillJobStateForTesting(
        jobId: String,
        status: BackfillJobStatus,
        progressCursor: BackfillProgressCursor?,
        retryCount: Int? = nil,
        deferReason: String? = nil
    ) throws {
        let sql = """
            UPDATE backfill_jobs
            SET status = ?,
                progressCursor = ?,
                retryCount = COALESCE(?, retryCount),
                deferReason = COALESCE(?, deferReason)
            WHERE jobId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, status.rawValue)
        bind(stmt, 2, try encodeJSONString(progressCursor))
        bind(stmt, 3, retryCount)
        bind(stmt, 4, deferReason)
        bind(stmt, 5, jobId)
        try step(stmt, expecting: SQLITE_DONE)
    }
    #endif

    @discardableResult
    func advanceBackfillJobPhase(
        jobId: String,
        expecting currentPhase: BackfillJobPhase,
        to nextPhase: BackfillJobPhase,
        status: BackfillJobStatus = .queued
    ) throws -> Bool {
        let sql = """
            UPDATE backfill_jobs
            SET phase = ?, progressCursor = NULL, status = ?, deferReason = NULL
            WHERE jobId = ? AND phase = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, nextPhase.rawValue)
        bind(stmt, 2, status.rawValue)
        bind(stmt, 3, jobId)
        bind(stmt, 4, currentPhase.rawValue)
        try step(stmt, expecting: SQLITE_DONE)
        return sqlite3_changes(db) > 0
    }

    // MARK: - Cohort GC

    /// Fix #4: deletes every `semantic_scan_results` and `evidence_events`
    /// row whose `scanCohortJSON` does not canonicalize to the supplied
    /// current cohort. Intended to be called once per app launch from
    /// `PlayheadRuntime.init` so old rows persisted under prior cohort
    /// hashes (e.g. after an app upgrade bumps the prompt hash, or a user
    /// changes locale) are reaped instead of accumulating forever.
    ///
    /// Both DELETEs run inside a single `BEGIN IMMEDIATE … COMMIT`
    /// transaction so a crash mid-prune cannot leave the two tables in
    /// divergent cohort states. Returns the total number of rows deleted
    /// across both tables, as reported by `sqlite3_changes`.
    ///
    /// NOTE: this method is exposed but NOT called by `migrate()`
    /// automatically. Wiring the production call in `PlayheadRuntime.init`
    /// is intentionally out of scope here (architectural; runtime changes
    /// are owned by a sibling agent). Call it once at app launch from the
    /// runtime with `ScanCohort.productionJSON()` as input.
    @discardableResult
    func pruneOrphanedScansForCurrentCohort(currentScanCohortJSON: String) throws -> Int {
        let canonical = Self.canonicalizeCohortJSON(currentScanCohortJSON)

        try exec("BEGIN IMMEDIATE")
        var totalDeleted = 0
        do {
            // Delete semantic scan rows under a non-current cohort.
            let scanSQL = "DELETE FROM semantic_scan_results WHERE scanCohortJSON != ?"
            let scanStmt = try prepare(scanSQL)
            bind(scanStmt, 1, canonical)
            try step(scanStmt, expecting: SQLITE_DONE)
            totalDeleted += Int(sqlite3_changes(db))
            sqlite3_finalize(scanStmt)

            // Delete evidence events under a non-current cohort.
            let evSQL = "DELETE FROM evidence_events WHERE scanCohortJSON != ?"
            let evStmt = try prepare(evSQL)
            bind(evStmt, 1, canonical)
            try step(evStmt, expecting: SQLITE_DONE)
            totalDeleted += Int(sqlite3_changes(db))
            sqlite3_finalize(evStmt)

            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }

        return totalDeleted
    }

    // MARK: - CRUD: semantic_scan_results

    /// Canonical column order shared by all `semantic_scan_results` readers:
    /// 0  id                         9  spansJSON           17 scanCohortJSON
    /// 1  analysisAssetId           10 status               18 transcriptVersion
    /// 2  windowFirstAtomOrdinal    11 attemptCount         19 reuseKeyHash
    /// 3  windowLastAtomOrdinal     12 errorContext         20 runMode (Rev3-M5)
    /// 4  windowStartTime           13 inputTokenCount      21 jobPhase (Rev3-M6)
    /// 5  windowEndTime             14 outputTokenCount
    /// 6  scanPass                  15 latencyMs
    /// 7  transcriptQuality         16 prewarmHit
    /// 8  disposition
    private static let semanticScanResultColumns = """
        id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
        windowStartTime, windowEndTime, scanPass, transcriptQuality,
        disposition, spansJSON, status, attemptCount, errorContext,
        inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
        scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase
        """

    /// H-1: canonicalize a `scanCohortJSON` before hashing so two
    /// semantically-equivalent cohorts with different key order or
    /// whitespace produce the same reuse key. Decodes to `ScanCohort` and
    /// re-encodes with `.sortedKeys`; if the decode fails, falls back to
    /// the raw string so the hash still diverges and the caller's malformed
    /// input cannot silently collide with valid rows.
    private static func canonicalizeCohortJSON(_ raw: String) -> String {
        guard let data = raw.data(using: .utf8),
              let decoded = try? JSONDecoder().decode(ScanCohort.self, from: data) else {
            return raw
        }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        guard let encoded = try? encoder.encode(decoded),
              let canonical = String(data: encoded, encoding: .utf8) else {
            return raw
        }
        return canonical
    }

    /// Computes the canonical reuse-key SHA-256 over the fields that govern
    /// FM scan reusability. The same field order is used in `fetchReusable…`,
    /// keeping inserts and lookups in lockstep. The `scanCohortJSON` field is
    /// canonicalized (sorted keys) before hashing so cohort-equivalent inputs
    /// collapse to the same hash regardless of upstream JSON formatting.
    ///
    /// H12 (cycle 2): `reuseScope` was added to the hash domain in bd-3vm
    /// to keep logically distinct jobs/phases (e.g. shadow vs. targeted)
    /// from collapsing each other when they share the same window bounds,
    /// scan pass, and transcript version. The string layout is
    ///   "<assetId>|<first>|<last>|<scanPass>|<transcriptVersion>|<canonicalCohort>|<scope>"
    /// where `scope` is `reuseScope ?? "default"`. **Pre-bd-3vm cached
    /// rows will not be reused** by post-bd-3vm callers because the hash
    /// domain expanded — those rows hash to the old layout (no scope
    /// segment) and never collide with the new lookups. Single user, full
    /// DB wipe on cohort change is acceptable, so we accept the cache
    /// miss instead of running a one-shot rehash migration.
    static func semanticScanReuseKeyHash(
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        scanPass: String,
        transcriptVersion: String,
        scanCohortJSON: String,
        reuseScope: String? = nil
    ) -> String {
        let canonicalCohort = canonicalizeCohortJSON(scanCohortJSON)
        let scope = reuseScope ?? "default"
        let canonical =
            "\(analysisAssetId)|\(windowFirstAtomOrdinal)|\(windowLastAtomOrdinal)|" +
            "\(scanPass)|\(transcriptVersion)|\(canonicalCohort)|\(scope)"
        let digest = SHA256.hash(data: Data(canonical.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    func insertSemanticScanResult(_ result: SemanticScanResult) throws {
        try validateScanCohortJSON(result.scanCohortJSON)

        // Fix #9: length caps on the two free-form blob columns. A runaway
        // error blob (e.g. a malformed model response echoed back verbatim)
        // or a bloated spansJSON payload would otherwise accumulate on disk
        // without bound. Reject at insert time so operators get a loud
        // signal instead of a silently growing SQLite file.
        //
        // NOTE: `BackfillJobRunner.isPermanent` already classifies
        // `insertFailed("payloadTooLarge: ...")` as a permanent error via
        // a string-prefix match, so oversized payloads are short-circuited
        // out of the retry budget without needing a dedicated enum case.
        let maxBlobLength = 1_000_000 // 1MB
        if let ctx = result.errorContext, ctx.utf8.count > maxBlobLength {
            throw AnalysisStoreError.insertFailed(
                "payloadTooLarge: errorContext \(ctx.utf8.count) bytes (max \(maxBlobLength))"
            )
        }
        if result.spansJSON.utf8.count > maxBlobLength {
            throw AnalysisStoreError.insertFailed(
                "payloadTooLarge: spansJSON \(result.spansJSON.utf8.count) bytes (max \(maxBlobLength))"
            )
        }

        let reuseKeyHash = Self.semanticScanReuseKeyHash(
            analysisAssetId: result.analysisAssetId,
            windowFirstAtomOrdinal: result.windowFirstAtomOrdinal,
            windowLastAtomOrdinal: result.windowLastAtomOrdinal,
            scanPass: result.scanPass,
            transcriptVersion: result.transcriptVersion,
            scanCohortJSON: result.scanCohortJSON,
            reuseScope: result.reuseScope
        )

        // H-1: a cached `.success` row must never be overwritten by a
        // subsequent `.refusal` (or other non-success) retry with the same
        // reuseKeyHash. The previous `INSERT OR REPLACE` silently destroyed
        // the cached success. Probe the existing row under the actor's
        // serialization guarantee and bail out early if the incoming row
        // would demote a cached success.
        //
        // Rank: `.success` outranks everything else. Same-rank collisions
        // fall through to the REPLACE path (last write wins), matching the
        // existing C5 contract for success-vs-success retries.
        if result.status != .success {
            let probe = try prepare("SELECT status FROM semantic_scan_results WHERE reuseKeyHash = ? LIMIT 1")
            defer { sqlite3_finalize(probe) }
            bind(probe, 1, reuseKeyHash)
            if sqlite3_step(probe) == SQLITE_ROW,
               let existingStatus = optionalText(probe, 0),
               existingStatus == SemanticScanStatus.success.rawValue {
                // Silently skip: the cached success is the canonical answer
                // and a later refusal must not destroy it.
                return
            }
        }

        let sql = """
            INSERT OR REPLACE INTO semantic_scan_results
            (id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
             windowStartTime, windowEndTime, scanPass, transcriptQuality,
             disposition, spansJSON, status, attemptCount, errorContext,
             inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
             scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, result.id)
        bind(stmt, 2, result.analysisAssetId)
        bind(stmt, 3, result.windowFirstAtomOrdinal)
        bind(stmt, 4, result.windowLastAtomOrdinal)
        bind(stmt, 5, result.windowStartTime)
        bind(stmt, 6, result.windowEndTime)
        bind(stmt, 7, result.scanPass)
        bind(stmt, 8, result.transcriptQuality.rawValue)
        bind(stmt, 9, result.disposition.rawValue)
        bind(stmt, 10, result.spansJSON)
        bind(stmt, 11, result.status.rawValue)
        bind(stmt, 12, result.attemptCount)
        bind(stmt, 13, result.errorContext)
        bind(stmt, 14, result.inputTokenCount)
        bind(stmt, 15, result.outputTokenCount)
        bind(stmt, 16, result.latencyMs)
        bind(stmt, 17, result.prewarmHit ? 1 : 0)
        bind(stmt, 18, result.scanCohortJSON)
        bind(stmt, 19, result.transcriptVersion)
        bind(stmt, 20, reuseKeyHash)
        bind(stmt, 21, result.runMode.rawValue)
        bind(stmt, 22, result.jobPhase)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// M2: atomic Pass-B write. Wraps a scan result and its evidence events in
    /// a single `BEGIN IMMEDIATE … COMMIT`. Any thrown error rolls back.
    @discardableResult
    func recordSemanticScanResult(
        _ result: SemanticScanResult,
        evidenceEvents: [EvidenceEvent]
    ) throws -> [String] {
        try validateScanCohortJSON(result.scanCohortJSON)
        for event in evidenceEvents {
            try validateAtomOrdinalsJSON(event.atomOrdinals)
            try validateScanCohortJSON(event.scanCohortJSON)
        }

        try exec("BEGIN IMMEDIATE")
        do {
            // R4-Fix2: when an incoming non-success row would be silently
            // dropped by `insertSemanticScanResult`'s H-1 success-protection
            // probe, the surrounding transaction must NOT commit the evidence
            // events — otherwise they attach to a phantom scan that the store
            // never wrote. Run the same check *inside* BEGIN IMMEDIATE so a
            // second writer cannot sneak in a cached success between the
            // preflight and the insert path.
            if result.status != .success {
                let reuseKeyHash = Self.semanticScanReuseKeyHash(
                    analysisAssetId: result.analysisAssetId,
                    windowFirstAtomOrdinal: result.windowFirstAtomOrdinal,
                    windowLastAtomOrdinal: result.windowLastAtomOrdinal,
                    scanPass: result.scanPass,
                    transcriptVersion: result.transcriptVersion,
                    scanCohortJSON: result.scanCohortJSON,
                    reuseScope: result.reuseScope
                )
                let probe = try prepare("SELECT status FROM semantic_scan_results WHERE reuseKeyHash = ? LIMIT 1")
                defer { sqlite3_finalize(probe) }
                bind(probe, 1, reuseKeyHash)
                if sqlite3_step(probe) == SQLITE_ROW,
                   let existingStatus = optionalText(probe, 0),
                   existingStatus == SemanticScanStatus.success.rawValue {
                    try exec("COMMIT")
                    return []
                }
            }

            var persistedEvidenceEventIds: [String] = []
            var seenEvidenceEventIds: Set<String> = []
            try insertSemanticScanResult(result)
            for event in evidenceEvents {
                if let persistedId = try insertEvidenceEvent(
                    event,
                    transcriptVersion: result.transcriptVersion
                ),
                   seenEvidenceEventIds.insert(persistedId).inserted {
                    persistedEvidenceEventIds.append(persistedId)
                }
            }
            try exec("COMMIT")
            return persistedEvidenceEventIds
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    func fetchSemanticScanResult(id: String) throws -> SemanticScanResult? {
        // Column order: see `semanticScanResultColumns` above.
        let sql = "SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results WHERE id = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, id)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readSemanticScanResult(stmt)
    }

    func fetchSemanticScanResults(
        analysisAssetId: String,
        scanPass: String? = nil
    ) throws -> [SemanticScanResult] {
        // Column order: see `semanticScanResultColumns` above.
        let sql: String
        if scanPass != nil {
            sql = """
                SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results
                WHERE analysisAssetId = ? AND scanPass = ?
                ORDER BY windowFirstAtomOrdinal ASC, rowid ASC
                """
        } else {
            sql = """
                SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results
                WHERE analysisAssetId = ?
                ORDER BY windowFirstAtomOrdinal ASC, rowid ASC
                """
        }
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        if let scanPass {
            bind(stmt, 2, scanPass)
        }
        var results: [SemanticScanResult] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(try readSemanticScanResult(stmt))
        }
        return results
    }

    /// C6/H-4: look up a reusable successful scan by computing the
    /// `reuseKeyHash` from the caller's tuple and hitting the
    /// `UNIQUE(reuseKeyHash)` index for a single O(log n) lookup. The
    /// previous implementation filtered on the (asset, ordinals, pass,
    /// transcriptVersion) tuple and iterated matching rows to compare
    /// cohort JSON in memory — correct but O(n) on the per-asset row set.
    ///
    /// Because inserts canonicalize cohort JSON before hashing (H-1) and
    /// this lookup does the same, cohort-equivalent strings always resolve
    /// to the same row regardless of upstream JSON formatting.
    func fetchReusableSemanticScanResult(
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        scanPass: String,
        scanCohortJSON: String,
        transcriptVersion: String,
        reuseScope: String? = nil
    ) throws -> SemanticScanResult? {
        let hash = Self.semanticScanReuseKeyHash(
            analysisAssetId: analysisAssetId,
            windowFirstAtomOrdinal: windowFirstAtomOrdinal,
            windowLastAtomOrdinal: windowLastAtomOrdinal,
            scanPass: scanPass,
            transcriptVersion: transcriptVersion,
            scanCohortJSON: scanCohortJSON,
            reuseScope: reuseScope
        )
        let sql = """
            SELECT \(Self.semanticScanResultColumns) FROM semantic_scan_results
            WHERE reuseKeyHash = ? AND status = 'success'
            LIMIT 1
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, hash)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readSemanticScanResult(stmt)
    }

    // MARK: - CRUD: evidence_events

    /// Canonical column order for `evidence_events` readers:
    /// 0 id, 1 analysisAssetId, 2 eventType, 3 sourceType,
    /// 4 atomOrdinals, 5 evidenceJSON, 6 scanCohortJSON, 7 createdAt,
    /// 8 runMode (Rev3-M5, shadow/targeted), 9 jobPhase (Rev3-M6,
    /// BackfillJobPhase.rawValue).
    private static let evidenceEventColumns = """
        id, analysisAssetId, eventType, sourceType,
        atomOrdinals, evidenceJSON, scanCohortJSON, createdAt, runMode, jobPhase
        """

    @discardableResult
    func insertEvidenceEvent(
        _ event: EvidenceEvent,
        transcriptVersion: String = ""
    ) throws -> String? {
        try validateAtomOrdinalsJSON(event.atomOrdinals)
        try validateScanCohortJSON(event.scanCohortJSON)
        // playhead-fn0: silent dedup on the exact persisted evidence identity:
        // (asset, eventType, sourceType, atomOrdinals, evidenceJSON,
        // scanCohortJSON, transcriptVersion). Distinct FM spans at the same
        // atom range now both persist when their bodies differ materially, and
        // append-only audit survives transcript-version churn.
        let sql = """
            INSERT OR IGNORE INTO evidence_events
            (id, analysisAssetId, eventType, sourceType, atomOrdinals,
             evidenceJSON, scanCohortJSON, transcriptVersion, createdAt, runMode, jobPhase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.eventType)
        bind(stmt, 4, event.sourceType.rawValue)
        bind(stmt, 5, event.atomOrdinals)
        bind(stmt, 6, event.evidenceJSON)
        bind(stmt, 7, event.scanCohortJSON)
        bind(stmt, 8, transcriptVersion)
        bind(stmt, 9, event.createdAt)
        bind(stmt, 10, event.runMode.rawValue)
        bind(stmt, 11, event.jobPhase)
        try step(stmt, expecting: SQLITE_DONE)
        if sqlite3_changes(db) > 0 {
            return event.id
        }

        // The row was ignored. Two legitimate cases remain:
        //   1. PRIMARY KEY collision with the *same* body — idempotent.
        //   2. Exact natural-key dedup where another row with the same
        //      persisted body already exists under the 6-column UNIQUE key.
        let probe = try prepare("""
            SELECT eventType, sourceType, atomOrdinals, evidenceJSON,
                   scanCohortJSON, transcriptVersion, createdAt, analysisAssetId
            FROM evidence_events
            WHERE id = ?
            LIMIT 1
            """)
        defer { sqlite3_finalize(probe) }
        bind(probe, 1, event.id)

        if sqlite3_step(probe) == SQLITE_ROW {
            let storedEventType = optionalText(probe, 0) ?? ""
            let storedSourceType = optionalText(probe, 1) ?? ""
            let storedAtomOrdinals = optionalText(probe, 2) ?? ""
            let storedEvidenceJSON = optionalText(probe, 3) ?? ""
            let storedScanCohortJSON = optionalText(probe, 4) ?? ""
            let storedTranscriptVersion = optionalText(probe, 5) ?? ""
            _ = sqlite3_column_double(probe, 6)
            let storedAnalysisAssetId = optionalText(probe, 7) ?? ""

            let bodyMatches =
                storedAnalysisAssetId == event.analysisAssetId &&
                storedEventType == event.eventType &&
                storedSourceType == event.sourceType.rawValue &&
                storedAtomOrdinals == event.atomOrdinals &&
                storedEvidenceJSON == event.evidenceJSON &&
                storedScanCohortJSON == event.scanCohortJSON &&
                storedTranscriptVersion == transcriptVersion

            if !bodyMatches {
                throw AnalysisStoreError.evidenceEventBodyMismatch(id: event.id)
            }
            return event.id
        }

        let naturalProbe = try prepare("""
            SELECT id
            FROM evidence_events
            WHERE analysisAssetId = ?
              AND eventType = ?
              AND sourceType = ?
              AND atomOrdinals = ?
              AND evidenceJSON = ?
              AND scanCohortJSON = ?
              AND transcriptVersion = ?
            LIMIT 1
            """)
        defer { sqlite3_finalize(naturalProbe) }
        bind(naturalProbe, 1, event.analysisAssetId)
        bind(naturalProbe, 2, event.eventType)
        bind(naturalProbe, 3, event.sourceType.rawValue)
        bind(naturalProbe, 4, event.atomOrdinals)
        bind(naturalProbe, 5, event.evidenceJSON)
        bind(naturalProbe, 6, event.scanCohortJSON)
        bind(naturalProbe, 7, transcriptVersion)

        if sqlite3_step(naturalProbe) == SQLITE_ROW {
            return optionalText(naturalProbe, 0) ?? event.id
        }

        logger.error(
            "evidence_events insert ignored without matching stored row: id=\(event.id, privacy: .public) eventType=\(event.eventType, privacy: .public)"
        )
        return nil
    }

    func fetchEvidenceEvents(analysisAssetId: String) throws -> [EvidenceEvent] {
        // Column order: see `evidenceEventColumns` above.
        let sql = """
            SELECT \(Self.evidenceEventColumns) FROM evidence_events
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC, rowid ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var events: [EvidenceEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            events.append(try readEvidenceEvent(stmt))
        }
        return events
    }

    // MARK: - JSON validation helpers

    /// M25/L-4: parses `atomOrdinals` and verifies it's a JSON array of
    /// integers. Uses `JSONDecoder.decode([Int].self, ...)` which rejects
    /// floats (`JSONSerialization` happily parses `1.5` as an `NSNumber`
    /// and a permissive numeric cast would let it through).
    private func validateAtomOrdinalsJSON(_ json: String) throws {
        guard let data = json.data(using: .utf8) else {
            throw AnalysisStoreError.invalidEvidenceEvent("atomOrdinals must be a JSON array of integers, got: \(json.prefix(80))")
        }
        do {
            _ = try JSONDecoder().decode([Int].self, from: data)
        } catch {
            throw AnalysisStoreError.invalidEvidenceEvent("atomOrdinals must be a JSON array of integers, got: \(json.prefix(80))")
        }
    }

    /// L-3: validates that `scanCohortJSON` decodes as a real `ScanCohort`
    /// object, not merely any parseable JSON value. The previous
    /// `JSONSerialization.jsonObject` check accepted top-level arrays,
    /// strings, or numbers, all of which are nonsensical cohorts and would
    /// silently defeat the reuse-key contract.
    private func validateScanCohortJSON(_ json: String) throws {
        guard let data = json.data(using: .utf8) else {
            throw AnalysisStoreError.invalidScanCohortJSON("not utf-8")
        }
        do {
            _ = try JSONDecoder().decode(ScanCohort.self, from: data)
        } catch {
            throw AnalysisStoreError.invalidScanCohortJSON("not a decodable ScanCohort: \(error)")
        }
    }

    private func readJob(_ stmt: OpaquePointer?) -> AnalysisJob {
        // playhead-uzdq: `generationID` and `schedulerEpoch` live at
        // trailing indices 21 and 22. They are `addColumnIfNeeded`
        // additions that SQLite appends to the end of the column list,
        // leaving the legacy positions 0..20 untouched. A pre-uzdq
        // schema that somehow lacked the columns would produce NULLs,
        // which we coerce to the "no-lease" sentinels via
        // `optionalText` / `optionalInt` + the nil-coalescing.
        AnalysisJob(
            jobId: text(stmt, 0),
            jobType: text(stmt, 1),
            episodeId: text(stmt, 2),
            podcastId: optionalText(stmt, 3),
            analysisAssetId: optionalText(stmt, 4),
            workKey: text(stmt, 5),
            sourceFingerprint: text(stmt, 6),
            downloadId: text(stmt, 7),
            priority: Int(sqlite3_column_int(stmt, 8)),
            desiredCoverageSec: sqlite3_column_double(stmt, 9),
            featureCoverageSec: sqlite3_column_double(stmt, 10),
            transcriptCoverageSec: sqlite3_column_double(stmt, 11),
            cueCoverageSec: sqlite3_column_double(stmt, 12),
            state: text(stmt, 13),
            attemptCount: Int(sqlite3_column_int(stmt, 14)),
            nextEligibleAt: optionalDouble(stmt, 15),
            leaseOwner: optionalText(stmt, 16),
            leaseExpiresAt: optionalDouble(stmt, 17),
            lastErrorCode: optionalText(stmt, 18),
            createdAt: sqlite3_column_double(stmt, 19),
            updatedAt: sqlite3_column_double(stmt, 20),
            generationID: optionalText(stmt, 21) ?? "",
            schedulerEpoch: optionalInt(stmt, 22) ?? 0
        )
    }

    private func readSemanticScanResult(_ stmt: OpaquePointer?) throws -> SemanticScanResult {
        let transcriptQualityRaw = try requireText(stmt, 7)
        guard let transcriptQuality = TranscriptQuality(rawValue: transcriptQualityRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown transcript quality '\(transcriptQualityRaw)'")
        }

        let dispositionRaw = try requireText(stmt, 8)
        guard let disposition = CoarseDisposition(rawValue: dispositionRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown coarse disposition '\(dispositionRaw)'")
        }

        let statusRaw = try requireText(stmt, 10)
        guard let status = SemanticScanStatus(rawValue: statusRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown semantic scan status '\(statusRaw)'")
        }

        // Rev3-M5: column 20 is `runMode`. Default to `.shadow` for any
        // legacy row that escaped the migration's NOT NULL DEFAULT.
        let runModeRaw = optionalText(stmt, 20) ?? SemanticScanPhase.shadow.rawValue
        let runMode = SemanticScanPhase(rawValue: runModeRaw) ?? .shadow

        return SemanticScanResult(
            id: try requireText(stmt, 0),
            analysisAssetId: try requireText(stmt, 1),
            windowFirstAtomOrdinal: Int(sqlite3_column_int(stmt, 2)),
            windowLastAtomOrdinal: Int(sqlite3_column_int(stmt, 3)),
            windowStartTime: sqlite3_column_double(stmt, 4),
            windowEndTime: sqlite3_column_double(stmt, 5),
            scanPass: try requireText(stmt, 6),
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: try requireText(stmt, 9),
            status: status,
            attemptCount: Int(sqlite3_column_int(stmt, 11)),
            errorContext: optionalText(stmt, 12),
            inputTokenCount: optionalInt(stmt, 13),
            outputTokenCount: optionalInt(stmt, 14),
            latencyMs: optionalDouble(stmt, 15),
            prewarmHit: sqlite3_column_int(stmt, 16) != 0,
            scanCohortJSON: try requireText(stmt, 17),
            transcriptVersion: try requireText(stmt, 18),
            // column 19 = reuseKeyHash (not persisted back onto the struct)
            runMode: runMode,
            jobPhase: optionalText(stmt, 21) ?? "shadow"
        )
    }

    private func readEvidenceEvent(_ stmt: OpaquePointer?) throws -> EvidenceEvent {
        let sourceTypeRaw = try requireText(stmt, 3)
        guard let sourceType = EvidenceSourceType(rawValue: sourceTypeRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown evidence source type '\(sourceTypeRaw)'")
        }

        // Rev3-M5: column 8 is `runMode`. Default to `.shadow` for any
        // legacy row that escaped the migration's NOT NULL DEFAULT.
        let runModeRaw = optionalText(stmt, 8) ?? SemanticScanPhase.shadow.rawValue
        let runMode = SemanticScanPhase(rawValue: runModeRaw) ?? .shadow

        return EvidenceEvent(
            id: try requireText(stmt, 0),
            analysisAssetId: try requireText(stmt, 1),
            eventType: try requireText(stmt, 2),
            sourceType: sourceType,
            atomOrdinals: try requireText(stmt, 4),
            evidenceJSON: try requireText(stmt, 5),
            scanCohortJSON: try requireText(stmt, 6),
            createdAt: sqlite3_column_double(stmt, 7),
            runMode: runMode,
            jobPhase: optionalText(stmt, 9) ?? "shadow"
        )
    }

    private func readBackfillJob(_ stmt: OpaquePointer?) throws -> BackfillJob {
        let phaseRaw = try requireText(stmt, 3)
        guard let phase = BackfillJobPhase(rawValue: phaseRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown backfill phase '\(phaseRaw)'")
        }

        let coveragePolicyRaw = try requireText(stmt, 4)
        guard let coveragePolicy = CoveragePolicy(rawValue: coveragePolicyRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown coverage policy '\(coveragePolicyRaw)'")
        }

        let statusRaw = try requireText(stmt, 9)
        guard let status = BackfillJobStatus(rawValue: statusRaw) else {
            throw AnalysisStoreError.queryFailed("Unknown backfill status '\(statusRaw)'")
        }

        return BackfillJob(
            jobId: try requireText(stmt, 0),
            analysisAssetId: try requireText(stmt, 1),
            podcastId: optionalText(stmt, 2),
            phase: phase,
            coveragePolicy: coveragePolicy,
            priority: Int(sqlite3_column_int(stmt, 5)),
            progressCursor: try decodeJSON(BackfillProgressCursor.self, from: optionalText(stmt, 6)),
            retryCount: Int(sqlite3_column_int(stmt, 7)),
            deferReason: optionalText(stmt, 8),
            status: status,
            scanCohortJSON: optionalText(stmt, 10),
            createdAt: sqlite3_column_double(stmt, 11)
        )
    }

    private func encodeMagnitudesJSON(_ magnitudes: [Float]) throws -> String {
        let data = try JSONEncoder().encode(magnitudes)
        guard let json = String(data: data, encoding: .utf8) else {
            throw AnalysisStoreError.insertFailed("Failed to encode feature extraction magnitudes as UTF-8")
        }
        return json
    }

    private func decodeMagnitudesJSON(_ json: String) throws -> [Float] {
        guard let data = json.data(using: .utf8) else {
            throw AnalysisStoreError.queryFailed("Feature extraction magnitudes JSON was not valid UTF-8")
        }
        do {
            return try JSONDecoder().decode([Float].self, from: data)
        } catch {
            throw AnalysisStoreError.queryFailed("Failed to decode feature extraction magnitudes JSON: \(error)")
        }
    }

    // MARK: - SQLite helpers

    private func exec(_ sql: String) throws {
        // playhead-6boz: every SQL surface routes through the lazy
        // bootstrap. Once `didOpen == true` this is a single bool check;
        // re-entrant calls from inside `runSchemaMigration` short-circuit
        // here because `ensureOpen()` flipped the flag before invoking
        // the migration body.
        try ensureOpen()
        var errMsg: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(db, sql, nil, nil, &errMsg)
        if rc != SQLITE_OK {
            let msg = errMsg.map { String(cString: $0) } ?? "unknown error"
            sqlite3_free(errMsg)
            throw AnalysisStoreError.migrationFailed("\(msg) (SQL: \(sql.prefix(120)))")
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer? {
        // playhead-6boz: see `exec` above for the lazy-bootstrap rationale.
        try ensureOpen()
        var stmt: OpaquePointer?
        let rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
        guard rc == SQLITE_OK else {
            // H9: SQLite may have allocated a partial statement before
            // failing — finalize unconditionally to avoid the leak.
            sqlite3_finalize(stmt)
            let msg = String(cString: sqlite3_errmsg(db))
            throw AnalysisStoreError.queryFailed("\(msg) (SQL: \(sql.prefix(120)))")
        }
        return stmt
    }

    private func step(_ stmt: OpaquePointer?, expecting expected: Int32) throws {
        let rc = sqlite3_step(stmt)
        guard rc == expected else {
            let msg = String(cString: sqlite3_errmsg(db))
            throw AnalysisStoreError.insertFailed(msg)
        }
    }

    // Bind helpers

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: String?) {
        if let value {
            // `withCString` guarantees the pointer is valid for the closure, and
            // `SQLITE_TRANSIENT` tells SQLite to copy the bytes immediately, so
            // no autoreleased NSString trampoline is needed per bind call.
            value.withCString { cstr in
                _ = sqlite3_bind_text(stmt, idx, cstr, -1, SQLITE_TRANSIENT_PTR)
            }
        } else {
            sqlite3_bind_null(stmt, idx)
        }
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: Double?) {
        if let value {
            sqlite3_bind_double(stmt, idx, value)
        } else {
            sqlite3_bind_null(stmt, idx)
        }
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: Int?) {
        if let value {
            sqlite3_bind_int(stmt, idx, Int32(value))
        } else {
            sqlite3_bind_null(stmt, idx)
        }
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: Int) {
        sqlite3_bind_int(stmt, idx, Int32(value))
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: Double) {
        sqlite3_bind_double(stmt, idx, value)
    }

    private func bind(_ stmt: OpaquePointer?, _ idx: Int32, _ value: String) {
        value.withCString { cstr in
            _ = sqlite3_bind_text(stmt, idx, cstr, -1, SQLITE_TRANSIENT_PTR)
        }
    }

    // Read helpers

    /// Read a NOT NULL text column. If the column is unexpectedly NULL, returns
    /// an empty string. Use ``optionalText(_:_:)`` for nullable columns.
    ///
    /// NOTE: This silent NULL → "" coercion is preserved for legacy readers
    /// (`readAsset`, `readSkipCue`, `readJob`, etc.) that are non-throwing.
    /// New code on the persistence boundary should call ``requireText(_:_:)``
    /// instead so an unexpected NULL throws `AnalysisStoreError.invalidRow`.
    private func text(_ stmt: OpaquePointer?, _ idx: Int32) -> String {
        sqlite3_column_text(stmt, idx).map { String(cString: $0) } ?? ""
    }

    /// M9: throwing variant of `text(_:_:)`. Throws
    /// `AnalysisStoreError.invalidRow` when a non-null column is unexpectedly
    /// NULL instead of masking the issue with an empty string.
    private func requireText(_ stmt: OpaquePointer?, _ idx: Int32) throws -> String {
        guard sqlite3_column_type(stmt, idx) != SQLITE_NULL,
              let cstr = sqlite3_column_text(stmt, idx) else {
            throw AnalysisStoreError.invalidRow(column: Int(idx))
        }
        return String(cString: cstr)
    }

    private func optionalText(_ stmt: OpaquePointer?, _ idx: Int32) -> String? {
        guard sqlite3_column_type(stmt, idx) != SQLITE_NULL else { return nil }
        return sqlite3_column_text(stmt, idx).map { String(cString: $0) }
    }

    private func optionalDouble(_ stmt: OpaquePointer?, _ idx: Int32) -> Double? {
        guard sqlite3_column_type(stmt, idx) != SQLITE_NULL else { return nil }
        return sqlite3_column_double(stmt, idx)
    }

    private func optionalInt(_ stmt: OpaquePointer?, _ idx: Int32) -> Int? {
        guard sqlite3_column_type(stmt, idx) != SQLITE_NULL else { return nil }
        return Int(sqlite3_column_int(stmt, idx))
    }

    private func encodeJSONString<T: Encodable>(_ value: T?) throws -> String? {
        guard let value else { return nil }
        let data = try JSONEncoder().encode(value)
        return String(decoding: data, as: UTF8.self)
    }

    private func decodeJSON<T: Decodable>(_ type: T.Type, from json: String?) throws -> T? {
        guard let json else { return nil }
        do {
            return try JSONDecoder().decode(T.self, from: Data(json.utf8))
        } catch {
            throw AnalysisStoreError.queryFailed("Failed to decode \(T.self): \(error)")
        }
    }

    // MARK: - CRUD: decoded_spans (Phase 5, playhead-4my.5.2)

    /// Persist decoded spans for an asset. Uses INSERT OR REPLACE for idempotency:
    /// re-running the decoder on the same input produces the same ids and overwrites
    /// existing rows without creating duplicates.
    func upsertDecodedSpans(_ spans: [DecodedSpan]) throws {
        guard !spans.isEmpty else { return }
        let encoder = JSONEncoder()
        try exec("BEGIN TRANSACTION")
        do {
            let sql = """
                INSERT OR REPLACE INTO decoded_spans
                (id, assetId, firstAtomOrdinal, lastAtomOrdinal, startTime, endTime, anchorProvenanceJSON)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            let stmt = try prepare(sql)
            defer { sqlite3_finalize(stmt) }
            for span in spans {
                let provenanceData = (try? encoder.encode(span.anchorProvenance)) ?? Data()
                let provenanceJSON = String(decoding: provenanceData, as: UTF8.self)
                sqlite3_reset(stmt)
                bind(stmt, 1, span.id)
                bind(stmt, 2, span.assetId)
                bind(stmt, 3, span.firstAtomOrdinal)
                bind(stmt, 4, span.lastAtomOrdinal)
                bind(stmt, 5, span.startTime)
                bind(stmt, 6, span.endTime)
                bind(stmt, 7, provenanceJSON)
                try step(stmt, expecting: SQLITE_DONE)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Fetch all decoded spans for an asset, ordered by startTime.
    func fetchDecodedSpans(assetId: String) throws -> [DecodedSpan] {
        let sql = """
            SELECT id, assetId, firstAtomOrdinal, lastAtomOrdinal,
                   startTime, endTime, anchorProvenanceJSON
            FROM decoded_spans
            WHERE assetId = ?
            ORDER BY startTime
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)

        let decoder = JSONDecoder()
        var results: [DecodedSpan] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let id = text(stmt, 0)
            let aid = text(stmt, 1)
            let firstOrdinal = Int(sqlite3_column_int(stmt, 2))
            let lastOrdinal = Int(sqlite3_column_int(stmt, 3))
            let startTime = sqlite3_column_double(stmt, 4)
            let endTime = sqlite3_column_double(stmt, 5)
            let provenanceJSON = text(stmt, 6)

            let provenance: [AnchorRef]
            if provenanceJSON.isEmpty || provenanceJSON == "[]" {
                provenance = []
            } else if let data = provenanceJSON.data(using: .utf8),
                      let wrapped = try? decoder.decode([LossyAnchorRef].self, from: data) {
                // Per-element tolerant decode: if a future build ships a new
                // AnchorRef case and the user rolls back, the unknown entries
                // are dropped individually rather than the whole span losing
                // all anchors.
                provenance = wrapped.compactMap(\.value)
            } else {
                logger.warning("fetchDecodedSpans: failed to decode anchorProvenanceJSON for span \(id, privacy: .public) asset \(aid, privacy: .public)")
                provenance = []
            }

            results.append(DecodedSpan(
                id: id,
                assetId: aid,
                firstAtomOrdinal: firstOrdinal,
                lastAtomOrdinal: lastOrdinal,
                startTime: startTime,
                endTime: endTime,
                anchorProvenance: provenance
            ))
        }
        return results
    }

    /// Delete all decoded spans for an asset. Used by tests and idempotent re-runs.
    func deleteDecodedSpans(assetId: String) throws {
        let sql = "DELETE FROM decoded_spans WHERE assetId = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        try step(stmt, expecting: SQLITE_DONE)
    }

    // MARK: - CRUD: ad_decision_results (Phase 6, playhead-4my.6.3)

    /// Upsert — a new cohort produces an updated decision for the same asset.
    ///
    /// The UNIQUE constraint on `analysisAssetId` means INSERT OR REPLACE overwrites the
    /// previous artifact row. Any `decision_events` rows written for the old artifact remain
    /// (append-only audit trail) and are now orphaned from the active artifact. This is
    /// intentional: callers querying events for a historical cohort can still find them by
    /// filtering on `decisionCohortJSON`. New-cohort callers should ignore old events.
    func saveDecisionResultArtifact(_ result: DecisionResultArtifact) throws {
        let sql = """
            INSERT OR REPLACE INTO ad_decision_results
            (id, analysisAssetId, decisionCohortJSON, inputArtifactRefs, decisionJSON, createdAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, result.id)
        bind(stmt, 2, result.analysisAssetId)
        bind(stmt, 3, result.decisionCohortJSON)
        bind(stmt, 4, result.inputArtifactRefs)
        bind(stmt, 5, result.decisionJSON)
        bind(stmt, 6, result.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func loadDecisionResultArtifact(for analysisAssetId: String) throws -> DecisionResultArtifact? {
        // ORDER BY / LIMIT are defensive no-ops: the UNIQUE constraint on analysisAssetId
        // guarantees at most one row per asset. They are harmless and clarify intent.
        let sql = "SELECT id, analysisAssetId, decisionCohortJSON, inputArtifactRefs, decisionJSON, createdAt FROM ad_decision_results WHERE analysisAssetId = ? ORDER BY createdAt DESC LIMIT 1"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try DecisionResultArtifact(
            id: requireText(stmt, 0),
            analysisAssetId: requireText(stmt, 1),
            decisionCohortJSON: requireText(stmt, 2),
            inputArtifactRefs: requireText(stmt, 3),
            decisionJSON: requireText(stmt, 4),
            createdAt: sqlite3_column_double(stmt, 5)
        )
    }

    // MARK: - CRUD: decision_events (append-only)

    func appendDecisionEvent(_ event: DecisionEvent) throws {
        let sql = """
            INSERT INTO decision_events
            (id, analysisAssetId, eventType, windowId, proposalConfidence, skipConfidence,
             eligibilityGate, policyAction, decisionCohortJSON, createdAt, explanationJSON)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.eventType)
        bind(stmt, 4, event.windowId)
        bind(stmt, 5, event.proposalConfidence)
        bind(stmt, 6, event.skipConfidence)
        bind(stmt, 7, event.eligibilityGate)
        bind(stmt, 8, event.policyAction)
        bind(stmt, 9, event.decisionCohortJSON)
        bind(stmt, 10, event.createdAt)
        bind(stmt, 11, event.explanationJSON)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func loadDecisionEvents(for analysisAssetId: String) throws -> [DecisionEvent] {
        // cycle-3 L2: include `rowid` as the final tiebreaker so two rows
        // sharing the same `createdAt` return in a deterministic order
        // (insertion order, since `rowid` is monotonically assigned for
        // INTEGER PRIMARY KEY-less tables). Downstream pickers in
        // `TrainingExampleMaterializer.bestDecision` break ties on
        // `skipConfidence` by taking the first match — without a stable
        // load order the choice was whatever the SQLite query planner
        // returned, which is not a contract we can rely on.
        let sql = "SELECT id, analysisAssetId, eventType, windowId, proposalConfidence, skipConfidence, eligibilityGate, policyAction, decisionCohortJSON, createdAt, explanationJSON FROM decision_events WHERE analysisAssetId = ? ORDER BY createdAt ASC, rowid ASC"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [DecisionEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(try DecisionEvent(
                id: requireText(stmt, 0),
                analysisAssetId: requireText(stmt, 1),
                eventType: requireText(stmt, 2),
                windowId: requireText(stmt, 3),
                proposalConfidence: sqlite3_column_double(stmt, 4),
                skipConfidence: sqlite3_column_double(stmt, 5),
                eligibilityGate: requireText(stmt, 6),
                policyAction: requireText(stmt, 7),
                decisionCohortJSON: requireText(stmt, 8),
                createdAt: sqlite3_column_double(stmt, 9),
                explanationJSON: optionalText(stmt, 10)
            ))
        }
        return results
    }

    // MARK: - CRUD: correction_events (Phase 7, playhead-4my.7.1)

    /// Persist a user correction event.
    func appendCorrectionEvent(_ event: CorrectionEvent) throws {
        let sql = """
            INSERT OR IGNORE INTO correction_events
            (id, analysisAssetId, scope, createdAt, source, podcastId,
             correctionType, causalSource, targetRefsJSON)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.scope)
        bind(stmt, 4, event.createdAt)
        bind(stmt, 5, event.source?.rawValue)
        bind(stmt, 6, event.podcastId)
        bind(stmt, 7, event.correctionType?.rawValue)
        bind(stmt, 8, event.causalSource?.rawValue)
        // Encode targetRefs to JSON if present.
        let targetRefsJSON: String? = {
            guard let refs = event.targetRefs else { return nil }
            guard let data = try? JSONEncoder().encode(refs) else { return nil }
            return String(data: data, encoding: .utf8)
        }()
        bind(stmt, 9, targetRefsJSON)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load all correction events for an asset, ordered by createdAt ascending.
    func loadCorrectionEvents(analysisAssetId: String) throws -> [CorrectionEvent] {
        let sql = """
            SELECT id, analysisAssetId, scope, createdAt, source, podcastId,
                   correctionType, causalSource, targetRefsJSON
            FROM correction_events
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)

        var results: [CorrectionEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let id = text(stmt, 0)
            let assetId = text(stmt, 1)
            let scope = text(stmt, 2)
            let createdAt = sqlite3_column_double(stmt, 3)
            let sourceRaw = optionalText(stmt, 4)
            let podcastId = optionalText(stmt, 5)
            let source = sourceRaw.flatMap { CorrectionSource(rawValue: $0) }
            let correctionTypeRaw = optionalText(stmt, 6)
            let causalSourceRaw = optionalText(stmt, 7)
            let targetRefsJSONStr = optionalText(stmt, 8)
            let correctionType = correctionTypeRaw.flatMap { CorrectionType(rawValue: $0) }
            let causalSource = causalSourceRaw.flatMap { CausalSource(rawValue: $0) }
            let targetRefs: CorrectionTargetRefs? = targetRefsJSONStr.flatMap { json in
                guard let data = json.data(using: .utf8) else { return nil }
                return try? JSONDecoder().decode(CorrectionTargetRefs.self, from: data)
            }
            results.append(CorrectionEvent(
                id: id,
                analysisAssetId: assetId,
                scope: scope,
                createdAt: createdAt,
                source: source,
                podcastId: podcastId,
                correctionType: correctionType,
                causalSource: causalSource,
                targetRefs: targetRefs
            ))
        }
        return results
    }

    /// Returns true if any correction event exists with the given scope string.
    func hasAnyCorrectionEvent(withScope scope: String) throws -> Bool {
        let sql = """
            SELECT EXISTS(
                SELECT 1 FROM correction_events WHERE scope = ?
            )
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, scope)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return false }
        return sqlite3_column_int(stmt, 0) != 0
    }

    /// Batch check: returns the set of scopes (from the input) that have at
    /// least one correction event. Single round-trip instead of N queries.
    func correctionScopesPresent(from scopes: [String]) throws -> Set<String> {
        guard !scopes.isEmpty else { return [] }
        let placeholders = scopes.map { _ in "?" }.joined(separator: ", ")
        let sql = "SELECT DISTINCT scope FROM correction_events WHERE scope IN (\(placeholders))"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        for (i, scope) in scopes.enumerated() {
            bind(stmt, Int32(i + 1), scope)
        }
        var result = Set<String>()
        while sqlite3_step(stmt) == SQLITE_ROW {
            result.insert(text(stmt, 0))
        }
        return result
    }

    // MARK: - CRUD: training_examples (playhead-4my.10.1)

    /// Canonical column order shared by all `training_examples` readers.
    /// Mirrors the table DDL exactly so positional binds and reads stay
    /// in sync without a `SELECT *` ordering hazard.
    private static let trainingExampleColumns = """
        id, analysisAssetId, startAtomOrdinal, endAtomOrdinal,
        transcriptVersion, startTime, endTime, textSnapshotHash,
        textSnapshot, bucket, commercialIntent, ownership,
        evidenceSourcesJSON, fmCertainty, classifierConfidence,
        userAction, eligibilityGate, scanCohortJSON,
        decisionCohortJSON, transcriptQuality, createdAt
        """

    /// Insert a single training example. Idempotent on `id` via
    /// `INSERT OR REPLACE` so re-materialization writes do not collide.
    func createTrainingExample(_ example: TrainingExample) throws {
        try insertTrainingExampleRow(example)
    }

    /// Batch insert. Wraps the inserts in a single transaction so a
    /// large materialization pass commits atomically (one fsync, one
    /// rollback boundary).
    func createTrainingExamples(_ examples: [TrainingExample]) throws {
        guard !examples.isEmpty else { return }
        try exec("BEGIN IMMEDIATE")
        do {
            for example in examples {
                try insertTrainingExampleRow(example)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Upsert each supplied training example for `analysisAssetId`.
    ///
    /// **Cohort-survival contract**: we deliberately do *not* DELETE the
    /// asset-scoped rows before inserting. Each `TrainingExample.id` is
    /// deterministic (`"te-\(scan.id)"`), so re-running the materializer on
    /// the same scan-result spine is naturally idempotent via
    /// `INSERT OR REPLACE`. Crucially, this means rows materialized under a
    /// previous cohort survive when the current cohort produces a smaller
    /// (or empty) spine — exactly the durability guarantee the bead is
    /// about. A blanket asset-scoped DELETE would wipe those prior-cohort
    /// rows on every re-run.
    ///
    /// Runs inside a single transaction so a partial failure rolls back
    /// cleanly.
    func replaceTrainingExamples(
        forAsset analysisAssetId: String,
        with examples: [TrainingExample]
    ) throws {
        guard !examples.isEmpty else { return }
        try exec("BEGIN IMMEDIATE")
        do {
            for example in examples {
                try insertTrainingExampleRow(example)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Load all training examples for an asset, ordered by createdAt
    /// ascending (then by row id for determinism on equal timestamps).
    func loadTrainingExamples(forAsset analysisAssetId: String) throws -> [TrainingExample] {
        let sql = """
            SELECT \(Self.trainingExampleColumns) FROM training_examples
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC, rowid ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)

        var results: [TrainingExample] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let example = readTrainingExample(stmt) else { continue }
            results.append(example)
        }
        return results
    }

    /// Load training examples scoped to (asset, scanCohortJSON). Used by
    /// downstream consumers that need provenance-bound exports — e.g.
    /// "give me only the rows produced under cohort X" so a cross-cohort
    /// comparison stays apples-to-apples. Equality is byte-exact on the
    /// stored JSON string; no canonicalization is attempted because the
    /// writer (materializer) always uses the value carried on the
    /// originating `semantic_scan_results` row, which itself was written
    /// by `ScanCohort.productionJSON()` (sorted-keys JSON).
    /// (playhead-4my.10.2)
    func loadTrainingExamples(
        forAsset analysisAssetId: String,
        scanCohortJSON: String
    ) throws -> [TrainingExample] {
        let sql = """
            SELECT \(Self.trainingExampleColumns) FROM training_examples
            WHERE analysisAssetId = ? AND scanCohortJSON = ?
            ORDER BY createdAt ASC, rowid ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        bind(stmt, 2, scanCohortJSON)

        var results: [TrainingExample] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let example = readTrainingExample(stmt) else { continue }
            results.append(example)
        }
        return results
    }

    private func insertTrainingExampleRow(_ example: TrainingExample) throws {
        let sql = """
            INSERT OR REPLACE INTO training_examples
            (\(Self.trainingExampleColumns))
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, example.id)
        bind(stmt, 2, example.analysisAssetId)
        bind(stmt, 3, example.startAtomOrdinal)
        bind(stmt, 4, example.endAtomOrdinal)
        bind(stmt, 5, example.transcriptVersion)
        bind(stmt, 6, example.startTime)
        bind(stmt, 7, example.endTime)
        bind(stmt, 8, example.textSnapshotHash)
        bind(stmt, 9, example.textSnapshot)
        bind(stmt, 10, example.bucket.rawValue)
        bind(stmt, 11, example.commercialIntent)
        bind(stmt, 12, example.ownership)
        // evidenceSources persisted as JSON array of strings.
        // L5: propagate encoder failure. `[String]` JSON encoding cannot
        // realistically fail; if it ever does, that's a Foundation
        // regression we want to see, not paper over with `"[]"`.
        let evidenceData = try JSONEncoder().encode(example.evidenceSources)
        guard let evidenceJSON = String(data: evidenceData, encoding: .utf8) else {
            // JSONEncoder always emits valid UTF-8; this is unreachable
            // unless Foundation breaks.
            throw AnalysisStoreError.encodingFailure(
                "training_examples.evidenceSourcesJSON: encoder returned non-UTF8 bytes"
            )
        }
        bind(stmt, 13, evidenceJSON)
        bind(stmt, 14, example.fmCertainty)
        bind(stmt, 15, example.classifierConfidence)
        bind(stmt, 16, example.userAction)
        bind(stmt, 17, example.eligibilityGate)
        bind(stmt, 18, example.scanCohortJSON)
        bind(stmt, 19, example.decisionCohortJSON)
        bind(stmt, 20, example.transcriptQuality)
        bind(stmt, 21, example.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    private func readTrainingExample(_ stmt: OpaquePointer?) -> TrainingExample? {
        guard let bucketRaw = optionalText(stmt, 9),
              let bucket = TrainingExampleBucket(rawValue: bucketRaw)
        else { return nil }
        let evidenceJSON = optionalText(stmt, 12) ?? "[]"
        let evidenceSources: [String] = {
            guard let data = evidenceJSON.data(using: .utf8) else { return [] }
            return (try? JSONDecoder().decode([String].self, from: data)) ?? []
        }()
        return TrainingExample(
            id: text(stmt, 0),
            analysisAssetId: text(stmt, 1),
            startAtomOrdinal: Int(sqlite3_column_int(stmt, 2)),
            endAtomOrdinal: Int(sqlite3_column_int(stmt, 3)),
            transcriptVersion: text(stmt, 4),
            startTime: sqlite3_column_double(stmt, 5),
            endTime: sqlite3_column_double(stmt, 6),
            textSnapshotHash: text(stmt, 7),
            textSnapshot: optionalText(stmt, 8),
            bucket: bucket,
            commercialIntent: text(stmt, 10),
            ownership: text(stmt, 11),
            evidenceSources: evidenceSources,
            fmCertainty: sqlite3_column_double(stmt, 13),
            classifierConfidence: sqlite3_column_double(stmt, 14),
            userAction: optionalText(stmt, 15),
            eligibilityGate: optionalText(stmt, 16),
            scanCohortJSON: text(stmt, 17),
            decisionCohortJSON: optionalText(stmt, 18),
            transcriptQuality: text(stmt, 19),
            createdAt: sqlite3_column_double(stmt, 20)
        )
    }

    // MARK: - CRUD: boundary_priors (ef2.3.5)

    /// Upsert a boundary prior distribution row. Uses INSERT OR REPLACE on the
    /// composite primary key (showId, edgeDirection, bracketTemplate).
    ///
    /// Note: SQLite's PRIMARY KEY on (showId, edgeDirection, bracketTemplate)
    /// treats NULL bracketTemplate values as distinct — each NULL is unique.
    /// We normalize NULL to the empty string "__none__" so upserts work correctly.
    func upsertBoundaryPrior(
        showId: String,
        edgeDirection: String,
        bracketTemplate: String?,
        median: Double,
        spread: Double,
        sampleCount: Int,
        lastUpdatedAt: Double
    ) throws {
        let sql = """
            INSERT INTO boundary_priors
            (showId, edgeDirection, bracketTemplate, median, spread, sampleCount, lastUpdatedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(showId, edgeDirection, bracketTemplate)
            DO UPDATE SET
                median = excluded.median,
                spread = excluded.spread,
                sampleCount = excluded.sampleCount,
                lastUpdatedAt = excluded.lastUpdatedAt
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, showId)
        bind(stmt, 2, edgeDirection)
        bind(stmt, 3, bracketTemplate ?? "__none__")
        bind(stmt, 4, median)
        bind(stmt, 5, spread)
        bind(stmt, 6, sampleCount)
        bind(stmt, 7, lastUpdatedAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load a single boundary prior distribution for the given context.
    func loadBoundaryPrior(
        showId: String,
        edgeDirection: String,
        bracketTemplate: String?
    ) throws -> BoundaryPriorDistribution? {
        let sql = """
            SELECT median, spread, sampleCount, lastUpdatedAt
            FROM boundary_priors
            WHERE showId = ? AND edgeDirection = ? AND bracketTemplate = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, showId)
        bind(stmt, 2, edgeDirection)
        bind(stmt, 3, bracketTemplate ?? "__none__")
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return BoundaryPriorDistribution(
            median: sqlite3_column_double(stmt, 0),
            spread: sqlite3_column_double(stmt, 1),
            sampleCount: Int(sqlite3_column_int(stmt, 2)),
            lastUpdatedAt: sqlite3_column_double(stmt, 3)
        )
    }

    /// Load all boundary priors for a given show, returned as a dictionary
    /// keyed by BoundaryPriorKey.
    func loadAllBoundaryPriors(forShow showId: String) throws -> [BoundaryPriorKey: BoundaryPriorDistribution] {
        let sql = """
            SELECT edgeDirection, bracketTemplate, median, spread, sampleCount, lastUpdatedAt
            FROM boundary_priors
            WHERE showId = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, showId)

        var result: [BoundaryPriorKey: BoundaryPriorDistribution] = [:]
        while sqlite3_step(stmt) == SQLITE_ROW {
            let edgeRaw = text(stmt, 0)
            let templateRaw = text(stmt, 1)
            guard let edge = EdgeDirection(rawValue: edgeRaw) else { continue }
            let template: String? = templateRaw == "__none__" ? nil : templateRaw
            let key = BoundaryPriorKey(showId: showId, edgeDirection: edge, bracketTemplate: template)
            let dist = BoundaryPriorDistribution(
                median: sqlite3_column_double(stmt, 2),
                spread: sqlite3_column_double(stmt, 3),
                sampleCount: Int(sqlite3_column_int(stmt, 4)),
                lastUpdatedAt: sqlite3_column_double(stmt, 5)
            )
            result[key] = dist
        }
        return result
    }

    // MARK: - CRUD: sponsor_knowledge_entries (Phase 8, playhead-4my.8.1)

    /// Upsert a sponsor knowledge entry. Uses INSERT OR REPLACE on the
    /// natural key (podcastId, entityType, normalizedValue).
    func upsertKnowledgeEntry(_ entry: SponsorKnowledgeEntry) throws {
        let sql = """
            INSERT INTO sponsor_knowledge_entries
            (id, podcastId, entityType, entityValue, normalizedValue, state,
             confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
             lastRollbackAt, decayedAt, blockedAt, aliases, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(podcastId, entityType, normalizedValue) DO UPDATE SET
                entityValue = excluded.entityValue,
                state = excluded.state,
                confirmationCount = excluded.confirmationCount,
                rollbackCount = excluded.rollbackCount,
                lastConfirmedAt = excluded.lastConfirmedAt,
                lastRollbackAt = excluded.lastRollbackAt,
                decayedAt = excluded.decayedAt,
                blockedAt = excluded.blockedAt,
                aliases = excluded.aliases,
                metadata = excluded.metadata
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, entry.id)
        bind(stmt, 2, entry.podcastId)
        bind(stmt, 3, entry.entityType.rawValue)
        bind(stmt, 4, entry.entityValue)
        bind(stmt, 5, entry.normalizedValue)
        bind(stmt, 6, entry.state.rawValue)
        bind(stmt, 7, entry.confirmationCount)
        bind(stmt, 8, entry.rollbackCount)
        bind(stmt, 9, entry.firstSeenAt)
        bind(stmt, 10, entry.lastConfirmedAt)
        bind(stmt, 11, entry.lastRollbackAt)
        bind(stmt, 12, entry.decayedAt)
        bind(stmt, 13, entry.blockedAt)
        let aliasesJSON = try encodeJSONString(entry.aliases)
        bind(stmt, 14, aliasesJSON)
        let metadataJSON = try encodeJSONString(entry.metadata)
        bind(stmt, 15, metadataJSON)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load a single knowledge entry by its natural key.
    func loadKnowledgeEntry(
        podcastId: String,
        entityType: KnowledgeEntityType,
        normalizedValue: String
    ) throws -> SponsorKnowledgeEntry? {
        let sql = """
            SELECT id, podcastId, entityType, entityValue, normalizedValue, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, aliases, metadata
            FROM sponsor_knowledge_entries
            WHERE podcastId = ? AND entityType = ? AND normalizedValue = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, entityType.rawValue)
        bind(stmt, 3, normalizedValue)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readKnowledgeEntry(stmt)
    }

    /// Load all knowledge entries for a podcast with a given state.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch, so one corrupt row doesn't break queries.
    func loadKnowledgeEntries(
        podcastId: String,
        state: KnowledgeState
    ) throws -> [SponsorKnowledgeEntry] {
        let sql = """
            SELECT id, podcastId, entityType, entityValue, normalizedValue, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, aliases, metadata
            FROM sponsor_knowledge_entries
            WHERE podcastId = ? AND state = ?
            ORDER BY firstSeenAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, state.rawValue)
        var results: [SponsorKnowledgeEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            do {
                results.append(try readKnowledgeEntry(stmt))
            } catch {
                logger.warning("Skipping corrupt knowledge entry: \(error.localizedDescription)")
            }
        }
        return results
    }

    /// Load all knowledge entries for a podcast regardless of state.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch.
    func loadAllKnowledgeEntries(podcastId: String) throws -> [SponsorKnowledgeEntry] {
        let sql = """
            SELECT id, podcastId, entityType, entityValue, normalizedValue, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, aliases, metadata
            FROM sponsor_knowledge_entries
            WHERE podcastId = ?
            ORDER BY firstSeenAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        var results: [SponsorKnowledgeEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            do {
                results.append(try readKnowledgeEntry(stmt))
            } catch {
                logger.warning("Skipping corrupt knowledge entry: \(error.localizedDescription)")
            }
        }
        return results
    }

    /// Read a SponsorKnowledgeEntry from the current row of a prepared statement.
    private func readKnowledgeEntry(_ stmt: OpaquePointer?) throws -> SponsorKnowledgeEntry {
        let id = text(stmt, 0)
        let podcastId = text(stmt, 1)
        let entityTypeRaw = text(stmt, 2)
        let entityValue = text(stmt, 3)
        let normalizedValue = text(stmt, 4)
        let stateRaw = text(stmt, 5)
        let confirmationCount = Int(sqlite3_column_int(stmt, 6))
        let rollbackCount = Int(sqlite3_column_int(stmt, 7))
        let firstSeenAt = sqlite3_column_double(stmt, 8)
        let lastConfirmedAt = optionalDouble(stmt, 9)
        let lastRollbackAt = optionalDouble(stmt, 10)
        let decayedAt = optionalDouble(stmt, 11)
        let blockedAt = optionalDouble(stmt, 12)
        let aliasesJSON = optionalText(stmt, 13)
        let metadataJSON = optionalText(stmt, 14)

        guard let entityType = KnowledgeEntityType(rawValue: entityTypeRaw) else {
            throw AnalysisStoreError.queryFailed("Invalid entityType: \(entityTypeRaw)")
        }
        guard let state = KnowledgeState(rawValue: stateRaw) else {
            throw AnalysisStoreError.queryFailed("Invalid KnowledgeState: \(stateRaw)")
        }

        let aliases: [String] = try decodeJSON([String].self, from: aliasesJSON) ?? []
        let metadata: [String: String]? = try decodeJSON([String: String].self, from: metadataJSON)

        return SponsorKnowledgeEntry(
            id: id,
            podcastId: podcastId,
            entityType: entityType,
            entityValue: entityValue,
            normalizedValue: normalizedValue,
            state: state,
            confirmationCount: confirmationCount,
            rollbackCount: rollbackCount,
            firstSeenAt: firstSeenAt,
            lastConfirmedAt: lastConfirmedAt,
            lastRollbackAt: lastRollbackAt,
            decayedAt: decayedAt,
            blockedAt: blockedAt,
            aliases: aliases,
            metadata: metadata
        )
    }

    // MARK: - CRUD: knowledge_candidate_events (Phase 8, playhead-4my.8.1)

    /// Append a knowledge candidate event (provenance log).
    func appendKnowledgeCandidateEvent(_ event: KnowledgeCandidateEvent) throws {
        let sql = """
            INSERT OR IGNORE INTO knowledge_candidate_events
            (id, analysisAssetId, entityType, entityValue, sourceAtomOrdinals,
             transcriptVersion, confidence, scanCohortJSON, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.entityType.rawValue)
        bind(stmt, 4, event.entityValue)
        let ordinalsJSON = try encodeJSONString(event.sourceAtomOrdinals)
        bind(stmt, 5, ordinalsJSON)
        bind(stmt, 6, event.transcriptVersion)
        bind(stmt, 7, event.confidence)
        bind(stmt, 8, event.scanCohortJSON)
        bind(stmt, 9, event.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load all candidate events for a given analysis asset, ordered by createdAt.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch.
    func loadKnowledgeCandidateEvents(analysisAssetId: String) throws -> [KnowledgeCandidateEvent] {
        let sql = """
            SELECT id, analysisAssetId, entityType, entityValue, sourceAtomOrdinals,
                   transcriptVersion, confidence, scanCohortJSON, createdAt
            FROM knowledge_candidate_events
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [KnowledgeCandidateEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let id = text(stmt, 0)
            let assetId = text(stmt, 1)
            let entityTypeRaw = text(stmt, 2)
            let entityValue = text(stmt, 3)
            let ordinalsJSON = text(stmt, 4)
            let transcriptVersion = text(stmt, 5)
            let confidence = sqlite3_column_double(stmt, 6)
            let scanCohortJSON = optionalText(stmt, 7)
            let createdAt = sqlite3_column_double(stmt, 8)

            guard let entityType = KnowledgeEntityType(rawValue: entityTypeRaw) else {
                logger.warning("Skipping candidate event with invalid entityType: \(entityTypeRaw)")
                continue
            }
            let ordinals: [Int] = (try? decodeJSON([Int].self, from: ordinalsJSON)) ?? []

            results.append(KnowledgeCandidateEvent(
                id: id,
                analysisAssetId: assetId,
                entityType: entityType,
                entityValue: entityValue,
                sourceAtomOrdinals: ordinals,
                transcriptVersion: transcriptVersion,
                confidence: confidence,
                scanCohortJSON: scanCohortJSON,
                createdAt: createdAt
            ))
        }
        return results
    }

    // MARK: - CRUD: ad_copy_fingerprints (Phase 9, playhead-4my.9.1)

    /// Upsert a fingerprint entry. Uses INSERT OR REPLACE on the
    /// natural key (podcastId, fingerprintHash).
    func upsertFingerprintEntry(_ entry: FingerprintEntry) throws {
        let sql = """
            INSERT INTO ad_copy_fingerprints
            (id, podcastId, fingerprintHash, normalizedText, state,
             confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
             lastRollbackAt, decayedAt, blockedAt, metadata,
             spanStartOffset, spanEndOffset, spanDurationSeconds,
             canonicalSponsorEntity, anchorLandmarks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(podcastId, fingerprintHash) DO UPDATE SET
                normalizedText = excluded.normalizedText,
                state = excluded.state,
                confirmationCount = excluded.confirmationCount,
                rollbackCount = excluded.rollbackCount,
                lastConfirmedAt = excluded.lastConfirmedAt,
                lastRollbackAt = excluded.lastRollbackAt,
                decayedAt = excluded.decayedAt,
                blockedAt = excluded.blockedAt,
                metadata = excluded.metadata,
                spanStartOffset = excluded.spanStartOffset,
                spanEndOffset = excluded.spanEndOffset,
                spanDurationSeconds = excluded.spanDurationSeconds,
                canonicalSponsorEntity = excluded.canonicalSponsorEntity,
                anchorLandmarks = excluded.anchorLandmarks
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, entry.id)
        bind(stmt, 2, entry.podcastId)
        bind(stmt, 3, entry.fingerprintHash)
        bind(stmt, 4, entry.normalizedText)
        bind(stmt, 5, entry.state.rawValue)
        bind(stmt, 6, entry.confirmationCount)
        bind(stmt, 7, entry.rollbackCount)
        bind(stmt, 8, entry.firstSeenAt)
        bind(stmt, 9, entry.lastConfirmedAt)
        bind(stmt, 10, entry.lastRollbackAt)
        bind(stmt, 11, entry.decayedAt)
        bind(stmt, 12, entry.blockedAt)
        let metadataJSON = try encodeJSONString(entry.metadata)
        bind(stmt, 13, metadataJSON)
        bind(stmt, 14, entry.spanStartOffset)
        bind(stmt, 15, entry.spanEndOffset)
        bind(stmt, 16, entry.spanDurationSeconds)
        bind(stmt, 17, entry.canonicalSponsorEntity?.value)
        let landmarksJSON = try encodeJSONString(entry.anchorLandmarks.isEmpty ? nil : entry.anchorLandmarks)
        bind(stmt, 18, landmarksJSON)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load a single fingerprint entry by its natural key.
    func loadFingerprintEntry(
        podcastId: String,
        fingerprintHash: String
    ) throws -> FingerprintEntry? {
        let sql = """
            SELECT id, podcastId, fingerprintHash, normalizedText, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, metadata,
                   spanStartOffset, spanEndOffset, spanDurationSeconds,
                   canonicalSponsorEntity, anchorLandmarks
            FROM ad_copy_fingerprints
            WHERE podcastId = ? AND fingerprintHash = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, fingerprintHash)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return try readFingerprintEntry(stmt)
    }

    /// Load all fingerprint entries for a podcast with a given state.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch.
    func loadFingerprintEntries(
        podcastId: String,
        state: KnowledgeState
    ) throws -> [FingerprintEntry] {
        let sql = """
            SELECT id, podcastId, fingerprintHash, normalizedText, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, metadata,
                   spanStartOffset, spanEndOffset, spanDurationSeconds,
                   canonicalSponsorEntity, anchorLandmarks
            FROM ad_copy_fingerprints
            WHERE podcastId = ? AND state = ?
            ORDER BY firstSeenAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, state.rawValue)
        var results: [FingerprintEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            do {
                results.append(try readFingerprintEntry(stmt))
            } catch {
                logger.warning("Skipping corrupt fingerprint entry: \(error.localizedDescription)")
            }
        }
        return results
    }

    /// Load all fingerprint entries for a podcast regardless of state.
    /// Rows with unrecognized enum values are skipped (logged) rather than
    /// failing the entire batch.
    func loadAllFingerprintEntries(podcastId: String) throws -> [FingerprintEntry] {
        let sql = """
            SELECT id, podcastId, fingerprintHash, normalizedText, state,
                   confirmationCount, rollbackCount, firstSeenAt, lastConfirmedAt,
                   lastRollbackAt, decayedAt, blockedAt, metadata,
                   spanStartOffset, spanEndOffset, spanDurationSeconds,
                   canonicalSponsorEntity, anchorLandmarks
            FROM ad_copy_fingerprints
            WHERE podcastId = ?
            ORDER BY firstSeenAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        var results: [FingerprintEntry] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            do {
                results.append(try readFingerprintEntry(stmt))
            } catch {
                logger.warning("Skipping corrupt fingerprint entry: \(error.localizedDescription)")
            }
        }
        return results
    }

    /// Read a FingerprintEntry from the current row of a prepared statement.
    private func readFingerprintEntry(_ stmt: OpaquePointer?) throws -> FingerprintEntry {
        let id = text(stmt, 0)
        let podcastId = text(stmt, 1)
        let fingerprintHash = text(stmt, 2)
        let normalizedText = text(stmt, 3)
        let stateRaw = text(stmt, 4)
        let confirmationCount = Int(sqlite3_column_int(stmt, 5))
        let rollbackCount = Int(sqlite3_column_int(stmt, 6))
        let firstSeenAt = sqlite3_column_double(stmt, 7)
        let lastConfirmedAt = optionalDouble(stmt, 8)
        let lastRollbackAt = optionalDouble(stmt, 9)
        let decayedAt = optionalDouble(stmt, 10)
        let blockedAt = optionalDouble(stmt, 11)
        let metadataJSON = optionalText(stmt, 12)

        guard let state = KnowledgeState(rawValue: stateRaw) else {
            throw AnalysisStoreError.queryFailed("Invalid KnowledgeState: \(stateRaw)")
        }

        let metadata: [String: String]? = try decodeJSON([String: String].self, from: metadataJSON)

        // B10: Read span offset columns (indices 13-17).
        let spanStartOffset = sqlite3_column_double(stmt, 13)
        let spanEndOffset = sqlite3_column_double(stmt, 14)
        let spanDurationSeconds = sqlite3_column_double(stmt, 15)
        let canonicalSponsorEntityRaw = optionalText(stmt, 16)
        let anchorLandmarksJSON = optionalText(stmt, 17)

        let canonicalSponsorEntity: NormalizedSponsor? = canonicalSponsorEntityRaw.map { NormalizedSponsor($0) }
        let anchorLandmarks: [AnchorLandmark] = (try decodeJSON([AnchorLandmark].self, from: anchorLandmarksJSON)) ?? []

        return FingerprintEntry(
            id: id,
            podcastId: podcastId,
            fingerprintHash: fingerprintHash,
            normalizedText: normalizedText,
            state: state,
            confirmationCount: confirmationCount,
            rollbackCount: rollbackCount,
            firstSeenAt: firstSeenAt,
            lastConfirmedAt: lastConfirmedAt,
            lastRollbackAt: lastRollbackAt,
            decayedAt: decayedAt,
            blockedAt: blockedAt,
            metadata: metadata,
            spanStartOffset: spanStartOffset,
            spanEndOffset: spanEndOffset,
            spanDurationSeconds: spanDurationSeconds,
            canonicalSponsorEntity: canonicalSponsorEntity,
            anchorLandmarks: anchorLandmarks
        )
    }

    // MARK: - Atomic confirm/rollback (Phase 9)

    /// Atomically load → increment confirmation → promote → upsert a fingerprint
    /// entry. Returns the resolved (podcastId, fingerprintHash) so the caller can
    /// log provenance against the correct stored hash. Because this runs inside a
    /// single actor-isolated call, no TOCTOU race is possible.
    func atomicConfirmFingerprint(
        podcastId: String,
        fingerprintHash: String,
        normalizedText: String,
        promote: (_ current: KnowledgeState, _ confirmations: Int, _ rollbacks: Int) -> KnowledgeState,
        nearDuplicateCheck: (_ newHash: String, _ existingHash: String) -> Bool
    ) throws -> (resolvedHash: String, entry: FingerprintEntry) {
        // 1. Exact match?
        if let existing = try loadFingerprintEntry(podcastId: podcastId, fingerprintHash: fingerprintHash) {
            // Blocked is truly terminal — return as-is to prevent the "new
            // entry" path from overwriting via UPSERT. Decayed entries CAN
            // recover through re-confirmation (by design).
            if existing.state == .blocked {
                return (fingerprintHash, existing)
            }
            let newCount = existing.confirmationCount + 1
            let now = Date().timeIntervalSince1970
            let newState = promote(existing.state, newCount, existing.rollbackCount)
            let updated = FingerprintEntry(
                id: existing.id,
                podcastId: existing.podcastId,
                fingerprintHash: existing.fingerprintHash,
                normalizedText: existing.normalizedText,
                state: newState,
                confirmationCount: newCount,
                rollbackCount: existing.rollbackCount,
                firstSeenAt: existing.firstSeenAt,
                lastConfirmedAt: now,
                lastRollbackAt: existing.lastRollbackAt,
                decayedAt: newState == .decayed ? now : existing.decayedAt,
                blockedAt: newState == .blocked ? now : existing.blockedAt,
                metadata: existing.metadata,
                spanStartOffset: existing.spanStartOffset,
                spanEndOffset: existing.spanEndOffset,
                spanDurationSeconds: existing.spanDurationSeconds,
                canonicalSponsorEntity: existing.canonicalSponsorEntity,
                anchorLandmarks: existing.anchorLandmarks
            )
            try upsertFingerprintEntry(updated)
            return (fingerprintHash, updated)
        }

        // 2. Near-duplicate match? Skip blocked entries — blocked is truly
        //    terminal and should not accumulate confirmations. Decayed entries
        //    can recover through re-confirmation.
        let allEntries = try loadAllFingerprintEntries(podcastId: podcastId)
        for entry in allEntries where entry.state != .blocked {
            if nearDuplicateCheck(fingerprintHash, entry.fingerprintHash) {
                let newCount = entry.confirmationCount + 1
                let now = Date().timeIntervalSince1970
                let newState = promote(entry.state, newCount, entry.rollbackCount)
                let updated = FingerprintEntry(
                    id: entry.id,
                    podcastId: entry.podcastId,
                    fingerprintHash: entry.fingerprintHash,
                    normalizedText: entry.normalizedText,
                    state: newState,
                    confirmationCount: newCount,
                    rollbackCount: entry.rollbackCount,
                    firstSeenAt: entry.firstSeenAt,
                    lastConfirmedAt: now,
                    lastRollbackAt: entry.lastRollbackAt,
                    decayedAt: newState == .decayed ? now : entry.decayedAt,
                    blockedAt: newState == .blocked ? now : entry.blockedAt,
                    metadata: entry.metadata,
                    spanStartOffset: entry.spanStartOffset,
                    spanEndOffset: entry.spanEndOffset,
                    spanDurationSeconds: entry.spanDurationSeconds,
                    canonicalSponsorEntity: entry.canonicalSponsorEntity,
                    anchorLandmarks: entry.anchorLandmarks
                )
                try upsertFingerprintEntry(updated)
                return (entry.fingerprintHash, updated)
            }
        }

        // 3. New entry.
        let now = Date().timeIntervalSince1970
        let initialState = promote(.candidate, 1, 0)
        let newEntry = FingerprintEntry(
            podcastId: podcastId,
            fingerprintHash: fingerprintHash,
            normalizedText: normalizedText,
            state: initialState,
            confirmationCount: 1,
            firstSeenAt: now,
            lastConfirmedAt: now
        )
        try upsertFingerprintEntry(newEntry)
        return (fingerprintHash, newEntry)
    }

    /// Atomically load → increment rollback → demote → upsert a fingerprint entry.
    func atomicRollbackFingerprint(
        podcastId: String,
        fingerprintHash: String,
        demote: (_ current: KnowledgeState, _ confirmations: Int, _ rollbacks: Int) -> KnowledgeState
    ) throws {
        guard let existing = try loadFingerprintEntry(podcastId: podcastId, fingerprintHash: fingerprintHash) else {
            return
        }
        let now = Date().timeIntervalSince1970
        let newRollbackCount = existing.rollbackCount + 1
        let newState = demote(existing.state, existing.confirmationCount, newRollbackCount)
        let updated = FingerprintEntry(
            id: existing.id,
            podcastId: existing.podcastId,
            fingerprintHash: existing.fingerprintHash,
            normalizedText: existing.normalizedText,
            state: newState,
            confirmationCount: existing.confirmationCount,
            rollbackCount: newRollbackCount,
            firstSeenAt: existing.firstSeenAt,
            lastConfirmedAt: existing.lastConfirmedAt,
            lastRollbackAt: now,
            decayedAt: newState == .decayed ? now : existing.decayedAt,
            blockedAt: newState == .blocked ? now : existing.blockedAt,
            metadata: existing.metadata,
            spanStartOffset: existing.spanStartOffset,
            spanEndOffset: existing.spanEndOffset,
            spanDurationSeconds: existing.spanDurationSeconds,
            canonicalSponsorEntity: existing.canonicalSponsorEntity,
            anchorLandmarks: existing.anchorLandmarks
        )
        try upsertFingerprintEntry(updated)
    }

    // MARK: - CRUD: fingerprint_source_events (Phase 9, playhead-4my.9.1)

    /// Append a fingerprint source event (provenance log).
    func appendFingerprintSourceEvent(_ event: FingerprintSourceEvent) throws {
        let sql = """
            INSERT OR IGNORE INTO fingerprint_source_events
            (id, analysisAssetId, fingerprintHash, sourceAdWindowId, confidence, createdAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.analysisAssetId)
        bind(stmt, 3, event.fingerprintHash)
        bind(stmt, 4, event.sourceAdWindowId)
        bind(stmt, 5, event.confidence)
        bind(stmt, 6, event.createdAt)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Load all source events for a given analysis asset, ordered by createdAt.
    func loadFingerprintSourceEvents(analysisAssetId: String) throws -> [FingerprintSourceEvent] {
        let sql = """
            SELECT id, analysisAssetId, fingerprintHash, sourceAdWindowId, confidence, createdAt
            FROM fingerprint_source_events
            WHERE analysisAssetId = ?
            ORDER BY createdAt ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [FingerprintSourceEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let id = text(stmt, 0)
            let assetId = text(stmt, 1)
            let fpHash = text(stmt, 2)
            let windowId = text(stmt, 3)
            let confidence = sqlite3_column_double(stmt, 4)
            let createdAt = sqlite3_column_double(stmt, 5)

            results.append(FingerprintSourceEvent(
                id: id,
                analysisAssetId: assetId,
                fingerprintHash: fpHash,
                sourceAdWindowId: windowId,
                confidence: confidence,
                createdAt: createdAt
            ))
        }
        return results
    }

    // MARK: - CRUD: music_bracket_trust (ef2.3.6)

    /// Load Beta parameters for a show's bracket trust. Returns nil if no record exists.
    func loadBracketTrust(forShow showId: String) throws -> BetaParameters? {
        let sql = "SELECT alpha, beta FROM music_bracket_trust WHERE showId = ?"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, showId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        let alpha = sqlite3_column_double(stmt, 0)
        let beta = sqlite3_column_double(stmt, 1)
        return BetaParameters(alpha: alpha, beta: beta)
    }

    /// Save or update Beta parameters for a show's bracket trust.
    func saveBracketTrust(showId: String, alpha: Double, beta: Double) throws {
        let sql = """
            INSERT INTO music_bracket_trust (showId, alpha, beta)
            VALUES (?, ?, ?)
            ON CONFLICT(showId) DO UPDATE SET alpha = excluded.alpha, beta = excluded.beta
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, showId)
        bind(stmt, 2, alpha)
        bind(stmt, 3, beta)
        try step(stmt, expecting: SQLITE_DONE)
    }

    // MARK: - Source Demotion Persistence (ef2.3.3)

    func loadSourceDemotionMultiplier(source: String, showId: String) throws -> Double? {
        let stmt = try prepare("""
            SELECT currentMultiplier FROM source_demotions
            WHERE showId = ? AND causalSource = ?
            """)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, showId)
        bind(stmt, 2, source)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return sqlite3_column_double(stmt, 0)
    }

    func upsertSourceDemotion(
        source: String,
        showId: String,
        currentMultiplier: Double,
        floor: Double,
        updatedAt: Double
    ) throws {
        let stmt = try prepare("""
            INSERT INTO source_demotions (showId, causalSource, currentMultiplier, floor, updatedAt)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(showId, causalSource)
            DO UPDATE SET currentMultiplier = excluded.currentMultiplier,
                         floor = excluded.floor,
                         updatedAt = excluded.updatedAt
            """)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, showId)
        bind(stmt, 2, source)
        bind(stmt, 3, currentMultiplier)
        bind(stmt, 4, floor)
        bind(stmt, 5, updatedAt)
        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_DONE else {
            throw AnalysisStoreError.queryFailed("upsertSourceDemotion: \(String(cString: sqlite3_errmsg(db)))")
        }
    }

    /// Confirmation threshold: how many confirmations clear a dispute.
    static let fingerprintDisputeConfirmationThreshold = 2

    func upsertFingerprintDispute(
        fingerprintId: String,
        showId: String,
        incrementDispute: Bool,
        incrementConfirmation: Bool,
        now: Double
    ) throws {
        // H2 fix: single atomic INSERT ... ON CONFLICT DO UPDATE avoids
        // the read-then-write TOCTOU race in the old load-then-branch pattern.
        let disputeInc = incrementDispute ? 1 : 0
        let confirmInc = incrementConfirmation ? 1 : 0
        let threshold = Self.fingerprintDisputeConfirmationThreshold

        let stmt = try prepare("""
            INSERT INTO fingerprint_disputes
                (fingerprintId, showId, disputeCount, confirmationCount, status, updatedAt)
            VALUES (?, ?, ?, ?, CASE WHEN ? >= ? THEN 'cleared' ELSE 'disputed' END, ?)
            ON CONFLICT(fingerprintId, showId)
            DO UPDATE SET
                disputeCount = fingerprint_disputes.disputeCount + excluded.disputeCount,
                confirmationCount = fingerprint_disputes.confirmationCount + excluded.confirmationCount,
                status = CASE
                    WHEN fingerprint_disputes.confirmationCount + excluded.confirmationCount >= ?
                    THEN 'cleared' ELSE 'disputed' END,
                updatedAt = excluded.updatedAt
            """)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, fingerprintId)
        bind(stmt, 2, showId)
        bind(stmt, 3, disputeInc)
        bind(stmt, 4, confirmInc)
        bind(stmt, 5, confirmInc) // for CASE in VALUES
        bind(stmt, 6, threshold)  // threshold for INSERT CASE
        bind(stmt, 7, now)
        bind(stmt, 8, threshold)  // threshold for ON CONFLICT CASE
        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_DONE else {
            throw AnalysisStoreError.queryFailed("upsertFingerprintDispute: \(String(cString: sqlite3_errmsg(db)))")
        }
    }

    func loadFingerprintDisputeStatus(fingerprintId: String, showId: String) throws -> String? {
        let stmt = try prepare("""
            SELECT status FROM fingerprint_disputes
            WHERE fingerprintId = ? AND showId = ?
            """)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, fingerprintId)
        bind(stmt, 2, showId)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        return optionalText(stmt, 0)
    }

    // loadFingerprintDisputeRow removed — upsertFingerprintDispute is now
    // a single atomic INSERT ... ON CONFLICT statement (H2 fix).

    // MARK: - CRUD: implicit_feedback_events (ef2.3.4)

    func appendImplicitFeedbackEvent(_ event: ImplicitFeedbackEvent) throws {
        let sql = """
            INSERT OR IGNORE INTO implicit_feedback_events
            (id, signal, analysisAssetId, podcastId, spanId, timestamp, weight)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, event.id)
        bind(stmt, 2, event.signal.rawValue)
        bind(stmt, 3, event.analysisAssetId)
        bind(stmt, 4, event.podcastId)
        bind(stmt, 5, event.spanId)
        bind(stmt, 6, event.timestamp)
        bind(stmt, 7, event.weight)
        try step(stmt, expecting: SQLITE_DONE)
    }

    func loadImplicitFeedbackEvents(analysisAssetId: String) throws -> [ImplicitFeedbackEvent] {
        let sql = """
            SELECT id, signal, analysisAssetId, podcastId, spanId, timestamp, weight
            FROM implicit_feedback_events
            WHERE analysisAssetId = ?
            ORDER BY timestamp ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, analysisAssetId)
        var results: [ImplicitFeedbackEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let signal = ImplicitFeedbackSignal(rawValue: text(stmt, 1)) else { continue }
            results.append(ImplicitFeedbackEvent(
                id: text(stmt, 0),
                signal: signal,
                analysisAssetId: text(stmt, 2),
                podcastId: optionalText(stmt, 3),
                spanId: optionalText(stmt, 4),
                timestamp: sqlite3_column_double(stmt, 5),
                storedWeight: sqlite3_column_double(stmt, 6)
            ))
        }
        return results
    }

    func loadImplicitFeedbackEvents(podcastId: String, limit: Int) throws -> [ImplicitFeedbackEvent] {
        let sql = """
            SELECT id, signal, analysisAssetId, podcastId, spanId, timestamp, weight
            FROM implicit_feedback_events
            WHERE podcastId = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, podcastId)
        bind(stmt, 2, limit)
        var results: [ImplicitFeedbackEvent] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let signal = ImplicitFeedbackSignal(rawValue: text(stmt, 1)) else { continue }
            results.append(ImplicitFeedbackEvent(
                id: text(stmt, 0),
                signal: signal,
                analysisAssetId: text(stmt, 2),
                podcastId: optionalText(stmt, 3),
                spanId: optionalText(stmt, 4),
                timestamp: sqlite3_column_double(stmt, 5),
                storedWeight: sqlite3_column_double(stmt, 6)
            ))
        }
        return results
    }

    // MARK: - CRUD: shadow_fm_responses (playhead-narl.2)
    //
    // The harness in `playhead-narl.1` reads these rows to replay the
    // `.allEnabled` config variant with real FM evidence for windows
    // that `.default` never scheduled FM on. No production pipeline
    // consumes these rows.

    /// Persist a shadow FM response. Uses `INSERT OR REPLACE` on the composite
    /// primary key (assetId, windowStart, windowEnd, configVariant) so Lane A
    /// and Lane B can both race to capture the same window without duplicate
    /// rows; the later write wins. The `capturedBy` column records which
    /// lane landed the final write.
    ///
    /// Rejects malformed windows (`windowEnd < windowStart`) — these
    /// degenerate into zero-width rows that no consumer can interpret.
    func upsertShadowFMResponse(_ row: ShadowFMResponse) throws {
        guard row.isWellFormed else {
            throw AnalysisStoreError.insertFailed(
                "shadow_fm_responses: non-well-formed row windowStart=\(row.windowStart) windowEnd=\(row.windowEnd)"
            )
        }
        // Empty fmResponse is meaningless — no harness consumer can
        // interpret a zero-length opaque BLOB, and a "successful capture"
        // with no payload would silently pollute the per-asset coverage
        // set. Reject explicitly rather than let it through as a
        // zero-length blob.
        guard !row.fmResponse.isEmpty else {
            throw AnalysisStoreError.insertFailed(
                "shadow_fm_responses: fmResponse payload is empty (asset=\(row.assetId))"
            )
        }
        // AC-6 PK canonicalization: round every REAL bound to the nearest
        // integer millisecond before binding. Integer-valued doubles below
        // 2^53 round-trip through Double exactly, so REAL PK equality is
        // stable at this resolution. This intentionally drops sub-ms
        // precision — the planner produces second-aligned windows so the
        // loss is zero in practice.
        let canonicalStart = ShadowFMResponse.canonicalize(seconds: row.windowStart)
        let canonicalEnd = ShadowFMResponse.canonicalize(seconds: row.windowEnd)
        let sql = """
            INSERT OR REPLACE INTO shadow_fm_responses
            (assetId, windowStart, windowEnd, configVariant,
             fmResponse, capturedAt, capturedBy, fmModelVersion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, row.assetId)
        bind(stmt, 2, canonicalStart)
        bind(stmt, 3, canonicalEnd)
        bind(stmt, 4, row.configVariant.rawValue)
        // Opaque BLOB payload — use SQLITE_TRANSIENT so sqlite copies the
        // bytes immediately and our `Data` can deallocate on return.
        let bytesCount = row.fmResponse.count
        row.fmResponse.withUnsafeBytes { rawBuf in
            if let base = rawBuf.baseAddress {
                _ = sqlite3_bind_blob(stmt, 5, base, Int32(bytesCount), SQLITE_TRANSIENT_PTR)
            } else {
                // Empty Data() — bind as a zero-length blob, not NULL.
                _ = sqlite3_bind_zeroblob(stmt, 5, 0)
            }
        }
        bind(stmt, 6, row.capturedAt)
        bind(stmt, 7, row.capturedBy.rawValue)
        bind(stmt, 8, row.fmModelVersion)
        try step(stmt, expecting: SQLITE_DONE)
    }

    /// Bulk insert helper — wraps the batch in a single transaction. Useful
    /// for Lane B's tick-driven backfill, which may land multiple rows back-
    /// to-back.
    func upsertShadowFMResponses(_ rows: [ShadowFMResponse]) throws {
        guard !rows.isEmpty else { return }
        try exec("BEGIN TRANSACTION")
        do {
            for row in rows {
                try upsertShadowFMResponse(row)
            }
            try exec("COMMIT")
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Fetch all shadow FM responses for an asset. Ordered by `windowStart`
    /// so the harness can walk them in timeline order. Rows with unrecognized
    /// `configVariant` or `capturedBy` strings are skipped (forward-compatible
    /// with future variant additions).
    func fetchShadowFMResponses(assetId: String) throws -> [ShadowFMResponse] {
        let sql = """
            SELECT assetId, windowStart, windowEnd, configVariant,
                   fmResponse, capturedAt, capturedBy, fmModelVersion
            FROM shadow_fm_responses
            WHERE assetId = ?
            ORDER BY windowStart ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        var results: [ShadowFMResponse] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let variant = ShadowConfigVariant(rawValue: text(stmt, 3)),
                  let capturedBy = ShadowCapturedBy(rawValue: text(stmt, 6))
            else { continue }
            // Pull the BLOB as Data. `sqlite3_column_bytes` gives the size in
            // bytes; `sqlite3_column_blob` gives a pointer valid until the
            // next `sqlite3_step`/`_reset`/`_finalize` on this stmt — copy
            // into a Data before the loop continues.
            let blobSize = Int(sqlite3_column_bytes(stmt, 4))
            let blob: Data
            if blobSize > 0, let ptr = sqlite3_column_blob(stmt, 4) {
                blob = Data(bytes: ptr, count: blobSize)
            } else {
                blob = Data()
            }
            results.append(ShadowFMResponse(
                assetId: text(stmt, 0),
                windowStart: sqlite3_column_double(stmt, 1),
                windowEnd: sqlite3_column_double(stmt, 2),
                configVariant: variant,
                fmResponse: blob,
                capturedAt: sqlite3_column_double(stmt, 5),
                capturedBy: capturedBy,
                fmModelVersion: optionalText(stmt, 7)
            ))
        }
        return results
    }

    /// Materialize every shadow FM response in the store, ordered by
    /// (assetId, windowStart). Used by the `shadow-decisions.jsonl` exporter
    /// via the ``ShadowDecisionsExportSource`` protocol.
    ///
    /// Memory cost scales with row count. At realistic volumes (thousands
    /// of rows) this is a few MB, acceptable for an export path that runs
    /// on demand.
    func allShadowFMResponses() throws -> [ShadowFMResponse] {
        let sql = """
            SELECT assetId, windowStart, windowEnd, configVariant,
                   fmResponse, capturedAt, capturedBy, fmModelVersion
            FROM shadow_fm_responses
            ORDER BY assetId ASC, windowStart ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        var results: [ShadowFMResponse] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let variant = ShadowConfigVariant(rawValue: text(stmt, 3)),
                  let capturedBy = ShadowCapturedBy(rawValue: text(stmt, 6))
            else { continue }
            let blobSize = Int(sqlite3_column_bytes(stmt, 4))
            let blob: Data
            if blobSize > 0, let ptr = sqlite3_column_blob(stmt, 4) {
                blob = Data(bytes: ptr, count: blobSize)
            } else {
                blob = Data()
            }
            results.append(ShadowFMResponse(
                assetId: text(stmt, 0),
                windowStart: sqlite3_column_double(stmt, 1),
                windowEnd: sqlite3_column_double(stmt, 2),
                configVariant: variant,
                fmResponse: blob,
                capturedAt: sqlite3_column_double(stmt, 5),
                capturedBy: capturedBy,
                fmModelVersion: optionalText(stmt, 7)
            ))
        }
        return results
    }

    /// The set of `(windowStart, windowEnd)` pairs already captured for
    /// `assetId` under `configVariant`. Lane A and Lane B both query this to
    /// avoid re-capturing a window that has already been shadowed.
    func capturedShadowWindows(
        assetId: String,
        configVariant: ShadowConfigVariant
    ) throws -> Set<ShadowWindowKey> {
        let sql = """
            SELECT windowStart, windowEnd FROM shadow_fm_responses
            WHERE assetId = ? AND configVariant = ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, assetId)
        bind(stmt, 2, configVariant.rawValue)
        var out: Set<ShadowWindowKey> = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            // Canonicalize on read so callers who canonicalize their own
            // prospective keys via `ShadowWindowKey.canonical(...)` get a
            // byte-for-byte-matching set membership test. Defensive: rows
            // written through `upsertShadowFMResponse` are already
            // canonical, but pre-canonicalization rows that might exist
            // in older DBs still normalize on read.
            out.insert(ShadowWindowKey.canonical(
                start: sqlite3_column_double(stmt, 0),
                end:   sqlite3_column_double(stmt, 1)
            ))
        }
        return out
    }

    /// Count of shadow responses currently persisted. Cheap — primarily a
    /// diagnostic helper for tests and the debug UI.
    func shadowFMResponseCount() throws -> Int {
        let sql = "SELECT COUNT(*) FROM shadow_fm_responses"
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// Delete every shadow FM response whose `fmModelVersion` is NOT the
    /// supplied current version. Used on boot to prune rows captured under
    /// a stale FM model after the app ships a newer one — the harness
    /// refuses to replay against a stale capture, so keeping the rows
    /// around wastes disk without any downstream benefit.
    ///
    /// Rows with a `NULL` `fmModelVersion` (legacy pre-versioning rows, if
    /// any ever appear) are considered stale by construction and deleted
    /// on this sweep. Returns the number of rows removed so callers can
    /// log the sweep's effect.
    @discardableResult
    func deleteShadowFMResponses(fmModelVersionOtherThan current: String) throws -> Int {
        let sql = """
            DELETE FROM shadow_fm_responses
            WHERE fmModelVersion IS NULL OR fmModelVersion != ?
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, current)
        try step(stmt, expecting: SQLITE_DONE)
        return Int(sqlite3_changes(db))
    }

    /// List the asset ids that have at least one transcript chunk and
    /// fewer `shadow_fm_responses` rows than the coarse grid would
    /// produce under `(strideSeconds, widthSeconds)`. Used by
    /// ``LiveShadowWindowSource.assetsWithIncompleteCoverage()`` to walk
    /// Lane B's backlog earliest-first.
    ///
    /// The expected-window formula mirrors ``LiveShadowWindowSource.gridWindows``:
    ///   - first window at `cursor = 0`
    ///   - each iteration advances `cursor += strideSeconds` and emits one
    ///     window while `cursor < duration`
    ///   - so `expectedWindows = ceil(duration / strideSeconds)` when
    ///     `duration > 0`, else `0`.
    ///
    /// `duration` for an asset is the MAX of `endTime` across its
    /// transcript chunks. Assets with no transcript rows are excluded
    /// (duration=0 → expectedWindows=0 → no incompleteness).
    ///
    /// Ordering: ascending by `analysis_assets.createdAt`, then `id` for
    /// stability.
    func assetsWithIncompleteShadowCoverage(
        strideSeconds: TimeInterval,
        widthSeconds: TimeInterval,
        configVariant: String
    ) throws -> [String] {
        precondition(strideSeconds > 0, "stride must be positive")
        precondition(widthSeconds > 0, "width must be positive")
        // Join assets → max(transcript_chunks.endTime) → count of
        // shadow_fm_responses for the given configVariant. Filter down
        // to the rows where shadowCount < expected.
        //
        // We use CAST to INTEGER via CEIL() emulation: SQLite doesn't
        // have CEIL, so we compute `((duration + stride - epsilon) /
        // stride)` and truncate. The epsilon guards against a duration
        // exactly divisible by stride producing an over-count.
        let sql = """
            SELECT a.id
            FROM analysis_assets a
            LEFT JOIN (
                SELECT analysisAssetId, MAX(endTime) AS duration
                FROM transcript_chunks
                GROUP BY analysisAssetId
            ) t ON t.analysisAssetId = a.id
            LEFT JOIN (
                SELECT assetId, COUNT(*) AS shadowCount
                FROM shadow_fm_responses
                WHERE configVariant = ?
                GROUP BY assetId
            ) s ON s.assetId = a.id
            WHERE t.duration IS NOT NULL
              AND t.duration > 0
              AND COALESCE(s.shadowCount, 0) <
                  CAST((t.duration + ? - 0.000001) / ? AS INTEGER)
            ORDER BY a.createdAt ASC, a.id ASC
            """
        let stmt = try prepare(sql)
        defer { sqlite3_finalize(stmt) }
        bind(stmt, 1, configVariant)
        sqlite3_bind_double(stmt, 2, strideSeconds)
        sqlite3_bind_double(stmt, 3, strideSeconds)
        var results: [String] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            results.append(text(stmt, 0))
        }
        return results
    }
}
