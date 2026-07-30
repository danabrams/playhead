// SpeechModelLoadRecord.swift
// The persisted — and exported — shape of "did the ASR model ever load".
//
// Scope: playhead-se2h (a swallowed launch-time model-load failure
//        permanently disabled transcription, with no retry).
//
// ----- Why this shape exists at all -----
//
// `SpeechService.loadFastModel()` is invoked once, at launch. Its failure
// used to land in an empty `catch` whose comment claimed the transcript
// engine would surface it later. It did not: every subsequent shard threw
// an indistinguishable `modelNotLoaded`, nothing retried, and the device
// transcribed nothing for the life of the install. There was no signal
// anywhere — not in the log, not on any surface, and not in a diagnostics
// bundle.
//
// The diagnostics bundle's `eligibility_snapshot` is NOT that signal and
// cannot be made into one: it reports Apple Intelligence availability, so
// a device whose ASR model has never loaded still reads fully eligible.
// This record is the missing half — what the SPEECH stack actually did.
//
// ----- Privacy -----
//
// Counters, dates, and rawValues of enums this repo defines. There is
// deliberately NO free-text field of any kind (not even the sanitised
// `detail` its sibling `AnalysisStoreHealthState` carries): a model that
// will not load has no episode, no show and no URL to name, so the
// safest vocabulary is a closed one with no escape hatch. Failure causes
// reuse `TranscriptFailureClass`, the closed set playhead-8ysk already
// pinned and privacy-reviewed.

import Foundation

// MARK: - Status

/// The device's standing with respect to loading an ASR model.
///
/// `unknown` is load-bearing and must stay the default for a missing
/// document. It is what distinguishes "this signal has never fired" —
/// a bundle from a build that predates it, or, far more usefully, a
/// wiring regression that stopped it firing — from "the model loaded
/// fine". A status enum whose default is a healthy value is exactly the
/// diagnostics field that reports health no matter how broken the device
/// is, which is the failure playhead-wvdz was filed to end.
enum SpeechModelLoadStatus: String, Codable, Sendable, CaseIterable {
    /// No load attempt has ever been recorded.
    ///
    /// On a device running a build that contains this signal, a launch
    /// that reached `prepareFastModel()` normally replaces this on its
    /// first attempt, so `unknown` in a bundle is strong evidence that
    /// nothing recorded — most often a wiring regression. It is NOT proof
    /// of one: an attempt cancelled on arrival records nothing by design,
    /// and a load still in flight has not concluded. So read it as "no
    /// determination has been made", never as "the device is healthy".
    case unknown
    /// The most recent recorded event was a successful load.
    case loaded
    /// At least one load has failed since the last success, but not
    /// enough consecutively to call the device persistently broken.
    case retrying
    /// Loads have failed ``SpeechModelLoadState/failuresBeforeConcern``
    /// or more times in a row with no success in between. Transcription
    /// is not happening on this device and has not been for a while.
    case persistentlyFailing = "persistently_failing"
}

// MARK: - Failure record

/// One recorded model-load failure. Four scalars, no strings.
struct SpeechModelLoadFailureRecord: Codable, Sendable, Equatable {
    let occurredAt: Date
    /// The closed cause vocabulary playhead-8ysk introduced for shard
    /// failures, reused verbatim. Sharing it is the point: a support
    /// engineer comparing `speech_model_load.recent_failures` against
    /// `work_journal_tail` is reading one taxonomy, not two.
    let failureClass: TranscriptFailureClass
    /// Which attempt within the process's retry budget this was (1-based).
    ///
    /// A gap in the sequence is meaningful and expected: attempts that end
    /// in cancellation spend a slot without recording anything, so a
    /// recorded `attemptNumber` of 3 with no 1 or 2 means the first two
    /// were cancelled rather than that records were lost. Treat it as
    /// "which slot this was", not as a count of recorded failures.
    let attemptNumber: Int
    /// The consecutive-failure counter AFTER this failure. Recorded so the
    /// escalation history can be reconstructed from the list alone — the
    /// same reason `AnalysisStoreFailureRecord` carries it.
    let consecutiveFailureCount: Int

    enum CodingKeys: String, CodingKey, CaseIterable {
        case occurredAt = "occurred_at"
        case failureClass = "failure_class"
        case attemptNumber = "attempt_number"
        case consecutiveFailureCount = "consecutive_failure_count"
    }
}

// MARK: - Document

/// The whole persisted document, and — unchanged — the shape that ships
/// in the diagnostics bundle under `speech_model_load`.
///
/// One type for both roles, for the reason `AnalysisStoreHealthState`
/// documents: a separate projection is a second place for the two to
/// drift, and there is nothing here that must not be exported.
struct SpeechModelLoadState: Codable, Sendable, Equatable {

    /// Failure records retained. Enough to see a pattern across several
    /// weeks of launches; small enough that the document stays tiny.
    static let maxFailureRecords = 20

    /// Consecutive failures before the status escalates to
    /// ``SpeechModelLoadStatus/persistentlyFailing``.
    ///
    /// Three, matching `AnalysisStoreHealthJournal.failuresBeforeAskingListener`
    /// deliberately — the two answer the same question about two
    /// subsystems, and a support engineer should not have to remember two
    /// thresholds. Note this counter is DURABLE and spans launches, while
    /// `SpeechService`'s retry budget is per-process: three failures in
    /// one launch and one failure in each of three launches are both real
    /// signals, and both should read the same way here.
    static let failuresBeforeConcern = 3

    let status: SpeechModelLoadStatus
    /// Consecutive failed loads since the last success, across launches.
    let consecutiveFailureCount: Int
    let firstFailureAt: Date?
    let lastFailureAt: Date?
    let lastSuccessAt: Date?
    /// The role loaded by the most recent SUCCESSFUL load.
    ///
    /// This is the field the bead was actually missing. `eligibility_snapshot`
    /// reports Apple Intelligence availability, not what the speech actor
    /// holds, so a device could read fully eligible while never having
    /// loaded a recognizer. Nil beside `status == .loaded` would be a
    /// contradiction; nil beside `.unknown` is simply "nothing yet".
    let lastSuccessfulRole: ModelRole?
    /// Newest last, capped at ``maxFailureRecords``.
    let recentFailures: [SpeechModelLoadFailureRecord]

    /// The empty document: nothing has ever been recorded.
    static let unknown = SpeechModelLoadState(
        status: .unknown,
        consecutiveFailureCount: 0,
        firstFailureAt: nil,
        lastFailureAt: nil,
        lastSuccessAt: nil,
        lastSuccessfulRole: nil,
        recentFailures: []
    )

    init(
        status: SpeechModelLoadStatus,
        consecutiveFailureCount: Int,
        firstFailureAt: Date?,
        lastFailureAt: Date?,
        lastSuccessAt: Date?,
        lastSuccessfulRole: ModelRole?,
        recentFailures: [SpeechModelLoadFailureRecord]
    ) {
        self.status = status
        self.consecutiveFailureCount = consecutiveFailureCount
        self.firstFailureAt = firstFailureAt
        self.lastFailureAt = lastFailureAt
        self.lastSuccessAt = lastSuccessAt
        self.lastSuccessfulRole = lastSuccessfulRole
        // Cap on the way IN, so no construction path — including a
        // decode of a document written by a build with a larger cap —
        // can produce an oversized document.
        self.recentFailures = recentFailures.suffix(Self.maxFailureRecords)
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case status
        case consecutiveFailureCount = "consecutive_failure_count"
        case firstFailureAt = "first_failure_at"
        case lastFailureAt = "last_failure_at"
        case lastSuccessAt = "last_success_at"
        case lastSuccessfulRole = "last_successful_role"
        case recentFailures = "recent_failures"
    }

    /// Field-by-field degrading decode, mirroring
    /// `AnalysisStoreHealthState`. A document written by a newer build,
    /// or a single corrupt field, must not cost the whole record — the
    /// counters are the part a support engineer needs most and they are
    /// the part most likely to survive.
    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.status = (try? container.decode(SpeechModelLoadStatus.self, forKey: .status)) ?? .unknown
        self.consecutiveFailureCount =
            (try? container.decode(Int.self, forKey: .consecutiveFailureCount)) ?? 0
        self.firstFailureAt = try? container.decodeIfPresent(Date.self, forKey: .firstFailureAt)
        self.lastFailureAt = try? container.decodeIfPresent(Date.self, forKey: .lastFailureAt)
        self.lastSuccessAt = try? container.decodeIfPresent(Date.self, forKey: .lastSuccessAt)
        self.lastSuccessfulRole = try? container.decodeIfPresent(ModelRole.self, forKey: .lastSuccessfulRole)
        let decodedFailures =
            (try? container.decode([SpeechModelLoadFailureRecord].self, forKey: .recentFailures)) ?? []
        self.recentFailures = decodedFailures.suffix(Self.maxFailureRecords)
    }
}
