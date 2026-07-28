// DiagnosticsBundle.swift
// Codable shapes for the support-safe diagnostics bundle. The default
// bundle is always emitted; the opt-in bundle is appended only when the
// user explicitly opts a given episode in via `Episode.diagnosticsOptIn`.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//        playhead-au2v.1.3 (chapter signal diagnostics events — additive
//        `chapter_phase_events` array on `DefaultBundle`, with a
//        `decodeIfPresent`-tolerant `init(from:)` so older fixtures
//        continue to decode unchanged).
//
// JSON key shape: every public field uses snake_case via explicit
// `CodingKeys`. The shape is part of the support engineer's contract —
// renaming a field is a breaking change for grep cheat sheets and any
// downstream tooling.
//
// `analysis_unavailable_reason` omission rule: when nil the key is OMITTED
// (not serialized as `null`). The encoder default already does this for
// `Encodable` optionals; we keep the field as `String?`-of-rawValue
// equivalent rather than promoting to a sentinel string so the omission
// stays automatic.
//
// Legal checklist alignment:
//   (a) The default bundle never includes a raw `episodeId`. Both the
//       scheduler-events tail and the work-journal tail emit
//       `episode_id_hash` only.
//   (b) Transcript excerpts in the OptIn bundle live under per-episode
//       gating and are bounded by the builder (±30s window, 1000-char
//       truncation).
//   (c) The hash itself is produced by `EpisodeIdHasher`; this file
//       only stores the resulting hex.
//   (d) `feature_summaries` is restricted here to coarse aggregates
//       (mean/max only). Adding a new metric requires explicit legal
//       review.
//   (e) `stability_diagnostics` (playhead-jw63.4) carries MetricKit
//       crash + hang records. `StabilityDiagnosticRecord` is a closed
//       shape with no free-text field; the allowlist that produces it
//       lives in `MetricKitDiagnosticProjector`.
//   (g) `banner_tallies` (playhead-bfq7) carries per-episode banner
//       card counts. Closed shape: one salted `episode_id_hash` (same
//       `EpisodeIdHasher` path as the scheduler-event tail) plus four
//       integers and two timestamps. No title, feed URL, advertiser,
//       product, window id, or transcript text — the raw episode id
//       stays behind in `BannerTallyStore` and is hashed by the
//       builder. Proof lives in `BannerTallyDiagnosticsPrivacyTests`.

import Foundation

// MARK: - BuildType

/// Build provenance for the support engineer. Kept as an explicit raw-value
/// enum (instead of a free-form String) so adding a value forces a touch
/// here and a corresponding update in `BuildType.detect()`.
enum BuildType: String, Sendable, Hashable, Codable, CaseIterable {
    case debug = "debug"
    case release = "release"
    case testFlight = "test_flight"

    /// Detect the active build type from `ProcessInfo` + bundle metadata.
    /// Order matters:
    ///   1. DEBUG configuration → `.debug` (compiled-in flag wins).
    ///   2. App Store receipt path containing "sandboxReceipt" →
    ///      `.testFlight` (the canonical TestFlight signal on iOS).
    ///   3. Otherwise → `.release`.
    static func detect(
        bundle: Bundle = .main,
        processInfo: ProcessInfo = .processInfo
    ) -> BuildType {
        #if DEBUG
        return .debug
        #else
        if let receiptURL = bundle.appStoreReceiptURL,
           receiptURL.lastPathComponent == "sandboxReceipt" {
            return .testFlight
        }
        _ = processInfo // reserved for future signals
        return .release
        #endif
    }
}

// MARK: - Default bundle

/// Always-safe bundle emitted on every diagnostics export. Contains no
/// per-episode content beyond the hashed `episode_id_hash` references
/// inside the scheduler-event tail and work-journal tail.
struct DefaultBundle: Codable, Sendable, Equatable {
    let appVersion: String
    let osVersion: String
    let deviceClass: DeviceClass
    let buildType: BuildType
    let eligibilitySnapshot: AnalysisEligibility
    let analysisUnavailableReason: AnalysisUnavailableReason?
    let schedulerEvents: [SchedulerEvent]
    let workJournalTail: [WorkJournalRecord]
    /// Chapter-phase diagnostic events (playhead-au2v.1.3). Sibling to
    /// `scheduler_events` and bumped on its own schedule. Empty array
    /// when no events have been emitted; we ALWAYS encode the key (even
    /// when empty) so support engineer's grep cheat sheet can rely on
    /// its presence in any v≥au2v.1.3 bundle. Older readers either
    /// already accept unknown keys (`JSONDecoder` default) or used
    /// `init(from:)` overloads that decode-if-present.
    let chapterPhaseEvents: [ChapterPhaseEvent]
    /// playhead-2hpn: one row per show with a recurring-jingle profile.
    /// Carries only the show identifier hash, the confirmation /
    /// consecutive-miss counts, the stored hash count, and the version
    /// stamp — never the raw audio-derived bits. Empty array when the
    /// `scopedMusicBedGeneralization` flag has never fired (or no shows
    /// have observed enough episodes). Legacy bundles missing this key
    /// decode as `[]` via the tolerant `init(from:)`.
    let musicBedProfiles: [MusicBedProfileSummary]

    /// playhead-beh3: per-device-class adaptive estimator state for the
    /// Welford+EWMA slice-sizing loop. Empty array when no rows exist
    /// (estimator never activated, or feature flag is OFF). ALWAYS
    /// encoded (even empty) so support engineer's grep cheat sheet can
    /// rely on its presence in any v≥beh3 bundle. Older readers either
    /// already accept unknown keys (`JSONDecoder` default) or use
    /// `init(from:)`'s `decodeIfPresent` fallback below.
    let learnedDeviceProfiles: [LearnedDeviceProfileDiagnosticRecord]

    /// playhead-jw63.4: the local MetricKit crash + hang ring buffer,
    /// newest first. ALWAYS encoded (empty array when the device has had
    /// no incidents, which is the healthy case) so a support engineer
    /// can distinguish "no crashes" from "this bundle predates the
    /// crash pipeline". Legacy bundles missing the key decode as `[]`.
    ///
    /// Privacy: every field of `StabilityDiagnosticRecord` is a number,
    /// a boolean, an enum rawValue, or a string that passed
    /// `DiagnosticTextSanitizer`'s character allowlist. It carries NO
    /// episode reference at all — not even a hash — because a stack
    /// trace has nothing to correlate to an episode.
    let stabilityDiagnostics: [StabilityDiagnosticRecord]

    /// playhead-bfq7: one row per episode listening session that put at
    /// least one banner card on screen, oldest first. ALWAYS encoded
    /// (empty array when no card has been presented) so a support
    /// engineer can distinguish "no banners" from "this bundle predates
    /// the tally". Legacy bundles missing the key decode as `[]`.
    ///
    /// Privacy: every field is an integer, a timestamp, or the salted
    /// `episode_id_hash` — the same install-scoped hex the
    /// scheduler-event tail uses (legal checklist item a). The raw
    /// episode id never reaches this struct; `DiagnosticsBundleBuilder`
    /// hashes it on the way in.
    let bannerTallies: [BannerTallySummary]

    /// playhead-p70f: the rediff re-fetch lane's telemetry — the cumulative
    /// bandwidth ledger, the per-asset lagged attempt states, the per-asset
    /// DAY-0 attempt records, and the lagged sweep's background-task fires.
    ///
    /// WHY IT IS HERE: this file contained ZERO rediff references, so three
    /// tables of decisive telemetry were being written on device and never
    /// exported. Establishing that day-0 had spent 299.6 MB while producing no
    /// ad windows required pulling the raw SQLite database off the phone. It
    /// should have taken a support bundle.
    ///
    /// Privacy: `analysisAssetId` is a local, install-scoped identifier, but it
    /// is hashed through `EpisodeIdHasher` on the way in anyway — the same
    /// treatment `music_bed_profiles` gives show identifiers (legal checklist
    /// item a). Everything else in the shape is an integer, a timestamp, or a
    /// closed enum rawValue. `last_detail` is the one free-text field and is
    /// sanitized + truncated by the builder.
    let rediffDiagnostics: RediffDiagnostics

    enum CodingKeys: String, CodingKey {
        case appVersion = "app_version"
        case osVersion = "os_version"
        case deviceClass = "device_class"
        case buildType = "build_type"
        case eligibilitySnapshot = "eligibility_snapshot"
        case analysisUnavailableReason = "analysis_unavailable_reason"
        case schedulerEvents = "scheduler_events"
        case workJournalTail = "work_journal_tail"
        case chapterPhaseEvents = "chapter_phase_events"
        case musicBedProfiles = "music_bed_profiles"
        case learnedDeviceProfiles = "learned_device_profiles"
        case stabilityDiagnostics = "stability_diagnostics"
        case bannerTallies = "banner_tallies"
        case rediffDiagnostics = "rediff_diagnostics"
    }

    init(
        appVersion: String,
        osVersion: String,
        deviceClass: DeviceClass,
        buildType: BuildType,
        eligibilitySnapshot: AnalysisEligibility,
        analysisUnavailableReason: AnalysisUnavailableReason?,
        schedulerEvents: [SchedulerEvent],
        workJournalTail: [WorkJournalRecord],
        chapterPhaseEvents: [ChapterPhaseEvent] = [],
        musicBedProfiles: [MusicBedProfileSummary] = [],
        learnedDeviceProfiles: [LearnedDeviceProfileDiagnosticRecord] = [],
        stabilityDiagnostics: [StabilityDiagnosticRecord] = [],
        bannerTallies: [BannerTallySummary] = [],
        rediffDiagnostics: RediffDiagnostics = .empty
    ) {
        self.rediffDiagnostics = rediffDiagnostics
        self.appVersion = appVersion
        self.osVersion = osVersion
        self.deviceClass = deviceClass
        self.buildType = buildType
        self.eligibilitySnapshot = eligibilitySnapshot
        self.analysisUnavailableReason = analysisUnavailableReason
        self.schedulerEvents = schedulerEvents
        self.workJournalTail = workJournalTail
        self.chapterPhaseEvents = chapterPhaseEvents
        self.musicBedProfiles = musicBedProfiles
        self.learnedDeviceProfiles = learnedDeviceProfiles
        self.stabilityDiagnostics = stabilityDiagnostics
        self.bannerTallies = bannerTallies
    }

    /// Decode-time tolerance for older bundles that pre-date the
    /// `chapter_phase_events` field: missing key decodes as `[]`.
    /// Existing fixtures (`sample-default-bundle.json`) and any bundle
    /// emitted before playhead-au2v.1.3 stay forward-compatible.
    /// Same tolerance for the playhead-beh3 `learned_device_profiles`
    /// field: missing key decodes as `[]`.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.appVersion = try container.decode(String.self, forKey: .appVersion)
        self.osVersion = try container.decode(String.self, forKey: .osVersion)
        self.deviceClass = try container.decode(DeviceClass.self, forKey: .deviceClass)
        self.buildType = try container.decode(BuildType.self, forKey: .buildType)
        self.eligibilitySnapshot = try container.decode(
            AnalysisEligibility.self, forKey: .eligibilitySnapshot
        )
        self.analysisUnavailableReason = try container.decodeIfPresent(
            AnalysisUnavailableReason.self, forKey: .analysisUnavailableReason
        )
        self.schedulerEvents = try container.decode(
            [SchedulerEvent].self, forKey: .schedulerEvents
        )
        self.workJournalTail = try container.decode(
            [WorkJournalRecord].self, forKey: .workJournalTail
        )
        self.chapterPhaseEvents = try container.decodeIfPresent(
            [ChapterPhaseEvent].self, forKey: .chapterPhaseEvents
        ) ?? []
        self.musicBedProfiles = try container.decodeIfPresent(
            [MusicBedProfileSummary].self, forKey: .musicBedProfiles
        ) ?? []
        self.learnedDeviceProfiles = try container.decodeIfPresent(
            [LearnedDeviceProfileDiagnosticRecord].self, forKey: .learnedDeviceProfiles
        ) ?? []
        self.stabilityDiagnostics = try container.decodeIfPresent(
            [StabilityDiagnosticRecord].self, forKey: .stabilityDiagnostics
        ) ?? []
        self.bannerTallies = try container.decodeIfPresent(
            [BannerTallySummary].self, forKey: .bannerTallies
        ) ?? []
        self.rediffDiagnostics = try container.decodeIfPresent(
            RediffDiagnostics.self, forKey: .rediffDiagnostics
        ) ?? .empty
    }

    // MARK: - playhead-p70f: rediff lane telemetry

    /// The rediff re-fetch lane, as a support engineer needs to read it.
    ///
    /// The four members answer, in order: "how much bandwidth has this lane
    /// spent and what did it produce?", "where is each episode in the lagged
    /// sweep's retry/backoff state machine?", "what happened on each play-time
    /// day-0 attempt, and how much did each episode cost?", and "is the lagged
    /// BGTask being granted any windows at all?".
    struct RediffDiagnostics: Codable, Sendable, Equatable {
        let bandwidth: RediffBandwidthSummary
        let refetchStates: [RediffRefetchStateSummary]
        let dayZeroAttempts: [RediffDayZeroAttemptSummary]
        let backgroundRuns: [RediffBackgroundRunSummary]

        static let empty = RediffDiagnostics(
            bandwidth: RediffBandwidthSummary(),
            refetchStates: [],
            dayZeroAttempts: [],
            backgroundRuns: []
        )

        enum CodingKeys: String, CodingKey {
            case bandwidth
            case refetchStates = "refetch_states"
            case dayZeroAttempts = "day_zero_attempts"
            case backgroundRuns = "background_runs"
        }
    }

    /// Cumulative `rediff_bandwidth_ledger` totals.
    ///
    /// HOW TO READ IT: `full_fetch_bytes_total` large with every outcome
    /// counter at zero is the playhead-p70f defect — bandwidth spent with no
    /// recorded result. `day_zero_unmarked_count` exists so that state is no
    /// longer expressible: a day-0 run that spends bytes and mints nothing
    /// increments it.
    struct RediffBandwidthSummary: Codable, Sendable, Equatable {
        var precheckBytesTotal: Int64 = 0
        var fullFetchBytesTotal: Int64 = 0
        var unchangedCount: Int = 0
        var rotatedCount: Int = 0
        var failedCount: Int = 0
        var parkedCount: Int = 0
        var dayZeroUnmarkedCount: Int = 0
        var lastUpdatedAt: Double?

        enum CodingKeys: String, CodingKey {
            case precheckBytesTotal = "precheck_bytes_total"
            case fullFetchBytesTotal = "full_fetch_bytes_total"
            case unchangedCount = "unchanged_count"
            case rotatedCount = "rotated_count"
            case failedCount = "failed_count"
            case parkedCount = "parked_count"
            case dayZeroUnmarkedCount = "day_zero_unmarked_count"
            case lastUpdatedAt = "last_updated_at"
        }
    }

    /// One `rediff_refetch_state` row — the LAGGED sweep's per-episode retry
    /// state. `resolved` means the lane is done with the episode.
    struct RediffRefetchStateSummary: Codable, Sendable, Equatable {
        let assetIdHash: String
        let unchangedAttempts: Int
        let lastAttemptAt: Double?
        let resolved: Bool
        let lastFailureClass: String?
        let sameClassFailureStreak: Int
        let updatedAt: Double

        enum CodingKeys: String, CodingKey {
            case assetIdHash = "asset_id_hash"
            case unchangedAttempts = "unchanged_attempts"
            case lastAttemptAt = "last_attempt_at"
            case resolved
            case lastFailureClass = "last_failure_class"
            case sameClassFailureStreak = "same_class_failure_streak"
            case updatedAt = "updated_at"
        }
    }

    /// One `rediff_day_zero_attempts` row — the play-time lane's per-episode
    /// history.
    ///
    /// HOW TO READ IT: `last_exit` names which of the formerly silent mint
    /// exits fired. When it is `no_accepted_byte_diff`, compare
    /// `last_b_sides_gate_rejected` against `last_b_side_count`: equal means
    /// the byte aligner rejected every copy (it is an MP3-frame parser, so
    /// non-MP3 bytes behind a normalized `.mp3` suffix look exactly like this),
    /// which is a different diagnosis from `no_divergent_slot` (copies diffed
    /// cleanly and agreed). `total_full_fetch_bytes` is the cumulative cost of
    /// this ONE episode; `suppressed_count` is how many replays were declined
    /// without spending anything.
    ///
    /// The on-device row's `lastDetail` (an error description that can carry
    /// the enclosure URL) is deliberately NOT projected here — see
    /// `DiagnosticsBundleBuilder.allowlistedRediffToken`.
    struct RediffDayZeroAttemptSummary: Codable, Sendable, Equatable {
        let assetIdHash: String
        let attemptCount: Int
        let lastAttemptAt: Double
        let lastExit: String
        let lastMarkCount: Int
        let lastBSideCount: Int
        let lastBSidesAccepted: Int
        let lastBSidesGateRejected: Int
        let lastBSidesUnreadable: Int
        let lastDivergentSlotCount: Int
        let lastFullFetchBytes: Int
        let totalFullFetchBytes: Int
        let suppressedCount: Int
        let lastSuppressedAt: Double?

        enum CodingKeys: String, CodingKey {
            case assetIdHash = "asset_id_hash"
            case attemptCount = "attempt_count"
            case lastAttemptAt = "last_attempt_at"
            case lastExit = "last_exit"
            case lastMarkCount = "last_mark_count"
            case lastBSideCount = "last_b_side_count"
            case lastBSidesAccepted = "last_b_sides_accepted"
            case lastBSidesGateRejected = "last_b_sides_gate_rejected"
            case lastBSidesUnreadable = "last_b_sides_unreadable"
            case lastDivergentSlotCount = "last_divergent_slot_count"
            case lastFullFetchBytes = "last_full_fetch_bytes"
            case totalFullFetchBytes = "total_full_fetch_bytes"
            case suppressedCount = "suppressed_count"
            case lastSuppressedAt = "last_suppressed_at"
        }
    }

    /// One `background_task_runs` row with `entryPoint = 'rediff_refetch'` —
    /// the LAGGED sweep's BGTask fires. A device where this array is nearly
    /// empty while other entry points have hundreds of rows is being starved of
    /// windows, not failing.
    struct RediffBackgroundRunSummary: Codable, Sendable, Equatable {
        let startedAt: Double
        let finishedAt: Double?
        let outcome: String
        let deferReason: String?
        let jobsSeen: Int?
        let jobsAdmitted: Int?
        let jobsCompleted: Int?
        let expiration: Bool

        enum CodingKeys: String, CodingKey {
            case startedAt = "started_at"
            case finishedAt = "finished_at"
            case outcome
            case deferReason = "defer_reason"
            case jobsSeen = "jobs_seen"
            case jobsAdmitted = "jobs_admitted"
            case jobsCompleted = "jobs_completed"
            case expiration
        }
    }

    /// Projected scheduler event derived from a `WorkJournalEntry` (per
    /// the bead spec: "NOT a new event stream"). The episodeId is hashed
    /// via `EpisodeIdHasher` before construction; the raw value never
    /// appears in this struct.
    struct SchedulerEvent: Codable, Sendable, Equatable {
        let timestamp: Double
        let eventType: String
        let episodeIdHash: String
        let internalMissCause: String?

        enum CodingKeys: String, CodingKey {
            case timestamp
            case eventType = "event_type"
            case episodeIdHash = "episode_id_hash"
            case internalMissCause = "internal_miss_cause"
        }
    }

    /// One row from the WorkJournal tail. `metadata` and `artifactClass`
    /// are deliberately omitted — they may carry PII (callers stash
    /// arbitrary JSON in `metadata`, and the artifact-class column adds
    /// no diagnostic value at the bundle layer).
    struct WorkJournalRecord: Codable, Sendable, Equatable {
        let id: String
        let episodeIdHash: String
        let generationID: String
        let schedulerEpoch: Int
        let timestamp: Double
        let eventType: String
        let cause: String?

        enum CodingKeys: String, CodingKey {
            case id
            case episodeIdHash = "episode_id_hash"
            case generationID = "generation_id"
            case schedulerEpoch = "scheduler_epoch"
            case timestamp
            case eventType = "event_type"
            case cause
        }
    }

    /// playhead-2hpn: support-safe summary of one show's recurring-jingle
    /// profile. The `show_identifier_hash` is the same install-scoped
    /// hex produced by `EpisodeIdHasher` (re-used here for shows: the
    /// hash is a per-install opaque key, NOT a globally identifiable
    /// catalogue token). The raw 64-bit jingle hashes themselves are
    /// deliberately omitted — they are derived from podcast audio and
    /// could in principle be back-correlated against a public catalogue
    /// fingerprint; only the COUNT is exposed.
    struct MusicBedProfileSummary: Codable, Sendable, Equatable {
        let showIdentifierHash: String
        let confirmationCount: Int
        let consecutiveMissCount: Int
        let storedHashCount: Int
        let isConfirmed: Bool
        let versionStamp: Int

        enum CodingKeys: String, CodingKey {
            case showIdentifierHash = "show_identifier_hash"
            case confirmationCount = "confirmation_count"
            case consecutiveMissCount = "consecutive_miss_count"
            case storedHashCount = "stored_hash_count"
            case isConfirmed = "is_confirmed"
            case versionStamp = "version_stamp"
        }
    }

    /// playhead-bfq7: banner cards presented during ONE listening
    /// session of one episode.
    ///
    /// HOW TO READ IT: the per-episode answer is the SUM of the rows
    /// sharing an `episode_id_hash`. More than one row can belong to a
    /// single listen — the playback lifecycle turns over on a re-tap of
    /// the already-playing episode and on every relaunch — so a row is
    /// a lower bound on a listen, never a duplicate of one. No card is
    /// counted twice under any of those splits; `first_shown_at` /
    /// `last_shown_at` are what separate "listened three times" from
    /// "one listen split three ways".
    ///
    /// `banner_count` is the number the audit reads;
    /// `suggest_count` is the share attributable to the tier that
    /// `markOnly` spans route to. The two per-tier counts always sum to
    /// `banner_count`.
    ///
    /// Deliberately absent: the episode title, the feed URL, the
    /// advertiser/product shown on the card, the orchestrator window
    /// ids, and the session key. Those exist on the local side of this
    /// projection (the `os_log` breadcrumb names the episode and the
    /// window) and stay there.
    struct BannerTallySummary: Codable, Sendable, Equatable {
        let episodeIdHash: String
        let bannerCount: Int
        let autoSkippedCount: Int
        let suggestCount: Int
        let firstShownAt: Double
        let lastShownAt: Double

        enum CodingKeys: String, CodingKey, CaseIterable {
            case episodeIdHash = "episode_id_hash"
            case bannerCount = "banner_count"
            case autoSkippedCount = "auto_skipped_count"
            case suggestCount = "suggest_count"
            case firstShownAt = "first_shown_at"
            case lastShownAt = "last_shown_at"
        }
    }
}

// MARK: - OptIn bundle

/// Per-episode bundle requiring explicit opt-in via
/// `Episode.diagnosticsOptIn`. The builder filters out non-opted
/// episodes; this struct only encodes what was passed in.
struct OptInBundle: Codable, Sendable, Equatable {
    let episodes: [Episode]

    enum CodingKeys: String, CodingKey {
        case episodes
    }

    /// One episode's worth of opted-in diagnostic context.
    struct Episode: Codable, Sendable, Equatable {
        let episodeId: String
        let episodeTitle: String
        let transcriptExcerpts: [TranscriptExcerpt]
        let featureSummaries: [FeatureSummary]

        enum CodingKeys: String, CodingKey {
            case episodeId = "episode_id"
            case episodeTitle = "episode_title"
            case transcriptExcerpts = "transcript_excerpts"
            case featureSummaries = "feature_summaries"
        }
    }

    /// ±30 s of transcript context around an ad boundary, with the raw
    /// excerpt truncated at 1000 chars. Construction is enforced through
    /// `DiagnosticsBundleBuilder` so the legal-checklist bounds (b) are
    /// applied uniformly.
    struct TranscriptExcerpt: Codable, Sendable, Equatable {
        let boundaryTime: Double
        let startTime: Double
        let endTime: Double
        let text: String

        enum CodingKeys: String, CodingKey {
            case boundaryTime = "boundary_time"
            case startTime = "start_time"
            case endTime = "end_time"
            case text
        }
    }

    /// Coarse-aggregate feature summary per episode. Restricted to means
    /// and one max — never raw vectors. New metrics require legal review
    /// (checklist item d).
    struct FeatureSummary: Codable, Sendable, Equatable {
        let rmsMean: Double
        let rmsMax: Double
        let spectralFluxMean: Double
        let musicProbabilityMean: Double
        let pauseProbabilityMean: Double

        enum CodingKeys: String, CodingKey {
            case rmsMean = "rms_mean"
            case rmsMax = "rms_max"
            case spectralFluxMean = "spectral_flux_mean"
            case musicProbabilityMean = "music_probability_mean"
            case pauseProbabilityMean = "pause_probability_mean"
        }
    }
}

// MARK: - Combined bundle file

/// Top-level wrapper written to disk as `playhead-diagnostics-<ISO8601>.json`.
/// Carrying both bundles in one file (rather than two attachments) keeps
/// the support engineer's flow single-artifact. When no episode opts in
/// the `optIn` field is omitted from the encoded JSON via the
/// `Encodable` optional convention.
struct DiagnosticsBundleFile: Codable, Sendable, Equatable {
    let generatedAt: Date
    let `default`: DefaultBundle
    let optIn: OptInBundle?

    enum CodingKeys: String, CodingKey {
        case generatedAt = "generated_at"
        case `default` = "default"
        case optIn = "opt_in"
    }
}
