// ChapterPhaseDiagnostics.swift
// Structured diagnostic events emitted by ChapterGenerationPhase and its
// downstream consumers. Mirrors the existing `scheduler_events` pattern
// (see `DiagnosticsBundle.SchedulerEvent` and `DiagnosticsBundleBuilder`):
// every emitted event carries a hashed `episode_id_hash`, a numeric
// `timestamp`, a snake_case `event_type` discriminator, and an optional
// per-event `payload` blob.
//
// Scope: playhead-au2v.1.3 (chapter signal diagnostics events).
//
// Why a sibling event stream instead of widening `scheduler_events`:
//   The bead spec is explicit — `chapter_phase_events` is a new top-level
//   array sibling to `scheduler_events`. Bumping its schema is independent
//   of the scheduler event wire (which is locked by support-engineer grep
//   contracts and by the legal checklist's hash-only invariant).
//
// Privacy invariant (legal checklist parity with scheduler_events):
//   * NO raw episode IDs — the only episode reference is the SHA-256 hex
//     hash produced by `EpisodeIdHasher`, identical to the convention
//     `scheduler_events` uses.
//   * NO episode titles, transcripts, or advertiser names anywhere in
//     payload fields. Payload structs are restricted to numeric counters,
//     boolean flags, snake_case enum raw values, and durations.
//   * If a future event needs to surface "what kind of failure"
//     information that is not safely encoded as a fixed-vocabulary enum
//     raw value, the new field MUST be reviewed against the same redaction
//     rules that govern `scheduler_events.internal_miss_cause`.
//
// Out-of-scope (later beads):
//   * Live emit call sites inside ChapterGenerationPhase (`.10`, `.12`,
//     `.13`), CoveragePlanner consumers (`.14`), FM-prompt consumers
//     (`.16`), narl-eval gate (`.18`). This file ships the event-type
//     definitions + `make…` factory helpers; the consumers will call
//     those helpers when those beads land.
//   * Persistence + retrieval. The current bead only defines the wire
//     format and the in-memory carrier. Wiring the chapter event stream
//     into AnalysisStore (mirroring `WorkJournalEntry`) is a separate
//     change once the consuming beads need it.

import Foundation

// MARK: - ChapterPhaseEventType

/// Discriminator for a `ChapterPhaseEvent`. Raw values are snake_case
/// strings so the serialised JSON matches the support-engineer grep
/// contract (`event_type == "chapter_phase_started"` etc.).
///
/// Adding a new case is a wire-format change. Bump the bead's schema
/// note and add a golden test alongside the existing per-event fixtures.
enum ChapterPhaseEventType: String, Sendable, Hashable, Codable, CaseIterable {
    /// Phase entered; payload carries the mode + transcript snapshot hash.
    case started = "chapter_phase_started"

    /// Phase short-circuited because the episode already exposes creator
    /// chapters via `ChapterSource ∈ {id3, pc20, rssInline}`. No FM cost.
    case skippedCreatorChapters = "chapter_phase_skipped_creator_chapters"

    /// `DeviceAdmissionPolicy` denied the phase (thermal pressure, FM
    /// unavailable, region/hardware unsupported, …). Payload carries
    /// the deny reason as a snake_case raw value.
    case skippedAdmission = "chapter_phase_skipped_admission"

    /// `ChapterBoundaryDetector` produced 0 candidates (short episode,
    /// monologue, pure music).
    case noCandidates = "chapter_phase_no_candidates"

    /// Candidate rate exceeded the pathological threshold (>1 per 90 s
    /// avg). Likely a detector glitch; phase aborted.
    case pathologicalRate = "chapter_phase_pathological_rate"

    /// Detected count exceeded target density; cap-and-merge applied.
    /// Payload records before/after counts.
    case capApplied = "chapter_phase_cap_applied"

    /// Per-call FM labelling failure (op-vs-semantic flag), retried,
    /// final outcome recorded.
    case labelFailed = "chapter_phase_label_failed"

    /// Operational-unclear rate exceeded threshold (>30%); plan dropped.
    case operationalUnclearRateExceeded = "chapter_phase_operational_unclear_rate_exceeded"

    /// Phase succeeded; plan written. Payload carries chapter count,
    /// plan confidence, FM-call count, and latency.
    case completed = "chapter_phase_completed"

    /// Backfill cancellation token fired mid-phase.
    case preempted = "chapter_phase_preempted"

    /// Cache decode failure on read (rare; emitted to detect cache
    /// corruption in field).
    case decodeFailure = "chapter_phase_decode_failure"
}

// MARK: - Payloads

/// Per-event payload blobs. Each struct is a closed schema (snake_case
/// CodingKeys, no free-form String/`Any` fields beyond the documented
/// per-event vocabulary). The wrapping `ChapterPhaseEvent.payload` field
/// carries `nil` when the event has nothing structured to add (e.g. a
/// stateless preemption).
///
/// All payload structs MUST stay PII-free. Reviewers should reject any
/// addition of `title`, `text`, `transcript`, `advertiser`, or any
/// String value sourced from episode metadata.
///
/// Asymmetry note: `ChapterPhaseEventType` has 11 cases, this enum has 9.
/// `noCandidates` and `preempted` are intentionally stateless — the
/// emit-helper factories on `ChapterPhaseEvent` set `payload: nil` for
/// those, and the encoded JSON omits the `payload` key entirely (see the
/// "Golden — chapter_phase_preempted" / "…no_candidates" tests). Adding
/// a payload to either event in a future bead is forwards-compatible:
/// the wrapping `ChapterPhaseEvent.payload` field is already `Optional`,
/// so older readers see the new key and ignore it.
enum ChapterPhasePayload: Sendable, Hashable, Equatable, Codable {

    case started(Started)
    case skippedCreatorChapters(SkippedCreatorChapters)
    case skippedAdmission(SkippedAdmission)
    case pathologicalRate(PathologicalRate)
    case capApplied(CapApplied)
    case labelFailed(LabelFailed)
    case operationalUnclearRateExceeded(OperationalUnclearRateExceeded)
    case completed(Completed)
    case decodeFailure(DecodeFailure)

    /// Phase-started payload. `transcriptSnapshotHash` is a content hash
    /// over the transcript at the moment the phase entered, NOT the
    /// transcript text. Used to detect "did the input change under us"
    /// during race-protection in the `.10` shell.
    struct Started: Sendable, Hashable, Equatable, Codable {
        /// Snake_case mode raw value (`heuristic_only`, `heuristic_plus_fm`,
        /// …) — the actual vocabulary lands with the phase shell in
        /// `playhead-au2v.1.10`. Free String today so this bead does not
        /// invent the enum prematurely.
        let mode: String
        /// SHA-256 hex of the transcript chunks visible at phase entry.
        /// Same shape as `episode_id_hash` (64 lowercase hex chars).
        let transcriptSnapshotHash: String

        enum CodingKeys: String, CodingKey {
            case mode
            case transcriptSnapshotHash = "transcript_snapshot_hash"
        }
    }

    /// Skipped because the episode already has creator-supplied chapters.
    struct SkippedCreatorChapters: Sendable, Hashable, Equatable, Codable {
        /// Snake_case `ChapterSource` raw value: `id3`, `pc20`, `rss_inline`.
        let chapterSource: String
        /// How many creator chapters were already present.
        let chapterCount: Int

        enum CodingKeys: String, CodingKey {
            case chapterSource = "chapter_source"
            case chapterCount = "chapter_count"
        }
    }

    /// Skipped because `DeviceAdmissionPolicy` denied the phase.
    struct SkippedAdmission: Sendable, Hashable, Equatable, Codable {
        /// Snake_case admission-deny reason raw value (e.g.
        /// `thermal_pressure`, `fm_unavailable`,
        /// `hardware_unsupported`, `region_unsupported`).
        let denyReason: String

        enum CodingKeys: String, CodingKey {
            case denyReason = "deny_reason"
        }
    }

    /// Pathological boundary rate detected; phase aborted.
    struct PathologicalRate: Sendable, Hashable, Equatable, Codable {
        let candidateCount: Int
        let episodeDurationSec: Double
        /// Candidates per second; reviewers can multiply by 90 to confirm
        /// the threshold was breached.
        let candidatesPerSecond: Double

        enum CodingKeys: String, CodingKey {
            case candidateCount = "candidate_count"
            case episodeDurationSec = "episode_duration_sec"
            case candidatesPerSecond = "candidates_per_second"
        }
    }

    /// Cap-and-merge applied because density exceeded target.
    struct CapApplied: Sendable, Hashable, Equatable, Codable {
        let detectedCount: Int
        let cappedCount: Int
        let targetDensity: Double

        enum CodingKeys: String, CodingKey {
            case detectedCount = "detected_count"
            case cappedCount = "capped_count"
            case targetDensity = "target_density"
        }
    }

    /// Per-call FM label failure with retry outcome.
    struct LabelFailed: Sendable, Hashable, Equatable, Codable {
        /// `true` for an operational failure (timeout, FM unavailable,
        /// transient platform error); `false` for a semantic failure
        /// (FM returned an unparseable / schema-violating answer).
        let operational: Bool
        /// Snake_case error code raw value (e.g. `fm_timeout`,
        /// `fm_decode_failure`). The exact vocabulary lands with the
        /// labelling service in `playhead-au2v.1.7`.
        let errorCode: String
        /// How many retries the labelling call burned before settling
        /// on `finalOutcome`.
        let retryCount: Int
        /// Snake_case final outcome (`success`, `gave_up`,
        /// `fell_back_to_heuristic`).
        let finalOutcome: String

        enum CodingKeys: String, CodingKey {
            case operational
            case errorCode = "error_code"
            case retryCount = "retry_count"
            case finalOutcome = "final_outcome"
        }
    }

    /// >30% of labelled chapters came back as operational-unclear; plan
    /// dropped.
    struct OperationalUnclearRateExceeded: Sendable, Hashable, Equatable, Codable {
        let labelledCount: Int
        let operationalUnclearCount: Int
        /// Fraction in [0, 1].
        let operationalUnclearRate: Double
        /// Configured threshold (e.g. 0.30) at the time of the event.
        let threshold: Double

        enum CodingKeys: String, CodingKey {
            case labelledCount = "labelled_count"
            case operationalUnclearCount = "operational_unclear_count"
            case operationalUnclearRate = "operational_unclear_rate"
            case threshold
        }
    }

    /// Phase completed successfully.
    struct Completed: Sendable, Hashable, Equatable, Codable {
        let chapterCount: Int
        /// Plan confidence in [0, 1].
        let planConfidence: Double
        let fmCallCount: Int
        /// Wall-clock latency from phase entry to plan write, in
        /// milliseconds.
        let latencyMs: Double

        enum CodingKeys: String, CodingKey {
            case chapterCount = "chapter_count"
            case planConfidence = "plan_confidence"
            case fmCallCount = "fm_call_count"
            case latencyMs = "latency_ms"
        }
    }

    /// Cache decode failure on read.
    struct DecodeFailure: Sendable, Hashable, Equatable, Codable {
        /// Snake_case stage raw value identifying which decoder failed
        /// (e.g. `chapter_plan_cache`, `boundary_candidates_cache`).
        let stage: String
        /// Snake_case error code (`corrupt_data`, `version_mismatch`,
        /// `truncated_payload`, …). Vocabulary tightens as cache shapes
        /// stabilise.
        let errorCode: String

        enum CodingKeys: String, CodingKey {
            case stage
            case errorCode = "error_code"
        }
    }

    // MARK: Codable round-trip
    //
    // We round-trip via a single-keyed object: the outer event already
    // carries the discriminator (`event_type`), so the payload simply
    // encodes / decodes whichever variant it holds. The encoded JSON
    // shape is `{"<payload_kind_key>": {…fields…}}`; this keeps the
    // schema self-describing without forcing the reader to inspect
    // `event_type` first. Skipped event types whose payload would be a
    // pure side-channel (`preempted`, `noCandidates`) intentionally have
    // NO `ChapterPhasePayload` case — the wrapping `ChapterPhaseEvent`
    // emits a missing `payload` key for those.

    private enum CodingKeys: String, CodingKey {
        case started
        case skippedCreatorChapters = "skipped_creator_chapters"
        case skippedAdmission = "skipped_admission"
        case pathologicalRate = "pathological_rate"
        case capApplied = "cap_applied"
        case labelFailed = "label_failed"
        case operationalUnclearRateExceeded = "operational_unclear_rate_exceeded"
        case completed
        case decodeFailure = "decode_failure"
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .started(let p):
            try container.encode(p, forKey: .started)
        case .skippedCreatorChapters(let p):
            try container.encode(p, forKey: .skippedCreatorChapters)
        case .skippedAdmission(let p):
            try container.encode(p, forKey: .skippedAdmission)
        case .pathologicalRate(let p):
            try container.encode(p, forKey: .pathologicalRate)
        case .capApplied(let p):
            try container.encode(p, forKey: .capApplied)
        case .labelFailed(let p):
            try container.encode(p, forKey: .labelFailed)
        case .operationalUnclearRateExceeded(let p):
            try container.encode(p, forKey: .operationalUnclearRateExceeded)
        case .completed(let p):
            try container.encode(p, forKey: .completed)
        case .decodeFailure(let p):
            try container.encode(p, forKey: .decodeFailure)
        }
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // Exactly-one-key invariant. We pick the FIRST present key so
        // unknown extra keys are tolerated forwards-compatibly; if no
        // known key is present we throw a typed decoding error.
        if let p = try container.decodeIfPresent(Started.self, forKey: .started) {
            self = .started(p)
        } else if let p = try container.decodeIfPresent(
            SkippedCreatorChapters.self, forKey: .skippedCreatorChapters
        ) {
            self = .skippedCreatorChapters(p)
        } else if let p = try container.decodeIfPresent(
            SkippedAdmission.self, forKey: .skippedAdmission
        ) {
            self = .skippedAdmission(p)
        } else if let p = try container.decodeIfPresent(
            PathologicalRate.self, forKey: .pathologicalRate
        ) {
            self = .pathologicalRate(p)
        } else if let p = try container.decodeIfPresent(
            CapApplied.self, forKey: .capApplied
        ) {
            self = .capApplied(p)
        } else if let p = try container.decodeIfPresent(
            LabelFailed.self, forKey: .labelFailed
        ) {
            self = .labelFailed(p)
        } else if let p = try container.decodeIfPresent(
            OperationalUnclearRateExceeded.self,
            forKey: .operationalUnclearRateExceeded
        ) {
            self = .operationalUnclearRateExceeded(p)
        } else if let p = try container.decodeIfPresent(
            Completed.self, forKey: .completed
        ) {
            self = .completed(p)
        } else if let p = try container.decodeIfPresent(
            DecodeFailure.self, forKey: .decodeFailure
        ) {
            self = .decodeFailure(p)
        } else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription:
                        "ChapterPhasePayload: no known payload variant key present"
                )
            )
        }
    }
}

// MARK: - ChapterPhaseEvent

/// One emitted chapter-phase event. Wire-shape parity with
/// `DefaultBundle.SchedulerEvent`: snake_case CodingKeys, hashed episode
/// id, numeric timestamp. The `payload` key is omitted when nil so
/// stateless events (`preempted`, `no_candidates`) do not carry a
/// dangling `null`.
struct ChapterPhaseEvent: Sendable, Hashable, Equatable, Codable {

    let timestamp: Double
    let eventType: ChapterPhaseEventType
    let episodeIdHash: String
    let payload: ChapterPhasePayload?

    enum CodingKeys: String, CodingKey {
        case timestamp
        case eventType = "event_type"
        case episodeIdHash = "episode_id_hash"
        case payload
    }

    // MARK: - Emit helpers
    //
    // Each `make…` factory accepts the raw `episodeId` + `installID` and
    // hashes them inside, so emit call sites can never accidentally ship
    // a raw id. The actual call sites land in later beads — the `// emitted by …`
    // comment marks which bead is responsible for wiring the helper.

    /// emitted by playhead-au2v.1.10 (ChapterGenerationPhase shell)
    static func started(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        mode: String,
        transcriptSnapshotHash: String
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .started,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .started(
                ChapterPhasePayload.Started(
                    mode: mode,
                    transcriptSnapshotHash: transcriptSnapshotHash
                )
            )
        )
    }

    /// emitted by playhead-au2v.1.10 (admission-check short-circuit)
    static func skippedCreatorChapters(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        chapterSource: String,
        chapterCount: Int
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .skippedCreatorChapters,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .skippedCreatorChapters(
                ChapterPhasePayload.SkippedCreatorChapters(
                    chapterSource: chapterSource,
                    chapterCount: chapterCount
                )
            )
        )
    }

    /// emitted by playhead-au2v.1.10 (admission-check short-circuit)
    static func skippedAdmission(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        denyReason: String
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .skippedAdmission,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .skippedAdmission(
                ChapterPhasePayload.SkippedAdmission(denyReason: denyReason)
            )
        )
    }

    /// emitted by playhead-au2v.1.4 (ChapterBoundaryDetector core algorithm)
    static func noCandidates(
        installID: UUID,
        episodeId: String,
        timestamp: Double
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .noCandidates,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: nil
        )
    }

    /// emitted by playhead-au2v.1.4 (pathological-rate guard in detector)
    static func pathologicalRate(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        candidateCount: Int,
        episodeDurationSec: Double,
        candidatesPerSecond: Double
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .pathologicalRate,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .pathologicalRate(
                ChapterPhasePayload.PathologicalRate(
                    candidateCount: candidateCount,
                    episodeDurationSec: episodeDurationSec,
                    candidatesPerSecond: candidatesPerSecond
                )
            )
        )
    }

    /// emitted by playhead-au2v.1.12 (cap-and-merge step)
    static func capApplied(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        detectedCount: Int,
        cappedCount: Int,
        targetDensity: Double
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .capApplied,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .capApplied(
                ChapterPhasePayload.CapApplied(
                    detectedCount: detectedCount,
                    cappedCount: cappedCount,
                    targetDensity: targetDensity
                )
            )
        )
    }

    /// emitted by playhead-au2v.1.7 (ChapterLabelingService)
    static func labelFailed(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        operational: Bool,
        errorCode: String,
        retryCount: Int,
        finalOutcome: String
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .labelFailed,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .labelFailed(
                ChapterPhasePayload.LabelFailed(
                    operational: operational,
                    errorCode: errorCode,
                    retryCount: retryCount,
                    finalOutcome: finalOutcome
                )
            )
        )
    }

    /// emitted by playhead-au2v.1.13 (operational-unclear gate)
    static func operationalUnclearRateExceeded(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        labelledCount: Int,
        operationalUnclearCount: Int,
        operationalUnclearRate: Double,
        threshold: Double
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .operationalUnclearRateExceeded,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .operationalUnclearRateExceeded(
                ChapterPhasePayload.OperationalUnclearRateExceeded(
                    labelledCount: labelledCount,
                    operationalUnclearCount: operationalUnclearCount,
                    operationalUnclearRate: operationalUnclearRate,
                    threshold: threshold
                )
            )
        )
    }

    /// emitted by playhead-au2v.1.13 (phase completion / plan write)
    static func completed(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        chapterCount: Int,
        planConfidence: Double,
        fmCallCount: Int,
        latencyMs: Double
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .completed,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .completed(
                ChapterPhasePayload.Completed(
                    chapterCount: chapterCount,
                    planConfidence: planConfidence,
                    fmCallCount: fmCallCount,
                    latencyMs: latencyMs
                )
            )
        )
    }

    /// emitted by playhead-au2v.1.10 (cancellation token plumbing)
    static func preempted(
        installID: UUID,
        episodeId: String,
        timestamp: Double
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .preempted,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: nil
        )
    }

    /// emitted by playhead-au2v.1.10 (cache-read decode failure path)
    static func decodeFailure(
        installID: UUID,
        episodeId: String,
        timestamp: Double,
        stage: String,
        errorCode: String
    ) -> ChapterPhaseEvent {
        ChapterPhaseEvent(
            timestamp: timestamp,
            eventType: .decodeFailure,
            episodeIdHash: EpisodeIdHasher.hash(installID: installID, episodeId: episodeId),
            payload: .decodeFailure(
                ChapterPhasePayload.DecodeFailure(
                    stage: stage,
                    errorCode: errorCode
                )
            )
        )
    }
}
