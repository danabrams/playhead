// DiagnosticsBundle.swift
// Codable shapes for the support-safe diagnostics bundle. The default
// bundle is always emitted; the opt-in bundle is appended only when the
// user explicitly opts a given episode in via `Episode.diagnosticsOptIn`.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
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

    enum CodingKeys: String, CodingKey {
        case appVersion = "app_version"
        case osVersion = "os_version"
        case deviceClass = "device_class"
        case buildType = "build_type"
        case eligibilitySnapshot = "eligibility_snapshot"
        case analysisUnavailableReason = "analysis_unavailable_reason"
        case schedulerEvents = "scheduler_events"
        case workJournalTail = "work_journal_tail"
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
