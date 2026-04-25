// SliceCompletionInstrumentation.swift
// playhead-1nl6: slice-completion instrumentation + InternalMissCause emission.
//
// This file owns three small, narrowly-scoped facilities:
//
//   1. `SliceMetadata` — the structured metadata blob appended to every
//      terminal (non-`acquired`/`checkpointed`) WorkJournal row. Serialized
//      to JSON and written to `work_journal.metadata`. The keys are the
//      contract spelled out in the bead plan (`slice_duration_ms`,
//      `bytes_processed`, `shards_completed`, `device_class`). Any
//      additional caller-specific keys can be attached via `extras`.
//
//   2. `SliceCounters` — an actor-isolated in-memory counter keyed by
//      `DeviceClass`. Fed from emission call sites; read at WorkJournal
//      append time so the metadata blob carries the aggregate row. The
//      counters reset on relaunch — WorkJournal rows are durable, the
//      aggregate is a convenience view only.
//
//   3. `CauseEmissionRegistry` — a process-wide registry of which
//      `InternalMissCause` variants have a live production emission path.
//      Production call sites register themselves at module init; the
//      enum-exhaustiveness test asserts every Phase-1 emitting variant has
//      at least one registered production tag. The registry is the
//      keystone test's compile-time safety net for "every cause has a
//      writer". Phase-1 non-emitters (`noRuntimeGrant`,
//      `modelTemporarilyUnavailable`, `unsupportedEpisodeLanguage`) do NOT
//      register — they are eligibility-layer concerns and have no live
//      slice path this phase.
//
// Emission channel (single source of truth) is the `cause` column on
// `WorkJournalEntry`. This file does not open a parallel event log; it
// enriches existing call sites. When multiple causes fire simultaneously,
// `CauseAttributionPolicy.resolve(causes:context:)` (already in repo via
// playhead-v11) picks the primary and the secondaries are dropped at this
// layer — only the primary is written to `work_journal.cause`.

import Foundation
import os

// MARK: - SliceMetadata

/// Structured metadata blob attached to every `preempted` / `failed` /
/// `finalized` WorkJournal row. Serialized to a compact JSON string via
/// `encode()` and written to the `work_journal.metadata` column.
///
/// Keys match the bead spec verbatim so downstream consumers
/// (playhead-5bb3, playhead-dfem) can decode without a schema file.
struct SliceMetadata: Codable, Sendable, Equatable {
    /// Wall-clock duration from lease acquisition to terminal event.
    let sliceDurationMs: Int

    /// Bytes the slice processed (feature extraction input, transcript
    /// chunks written, etc.). `0` is a legitimate value for a preempted
    /// slice that bailed before processing any data.
    let bytesProcessed: Int

    /// Number of shards the slice fully completed before termination.
    let shardsCompleted: Int

    /// `DeviceClass.rawValue` captured at emission time.
    let deviceClass: String

    /// Free-form extras (e.g. `cause` rehash, stack trace). Serialized
    /// as sibling keys under the same top-level JSON object so the
    /// top-level shape stays flat for consumers that look up keys by
    /// name.
    let extras: [String: String]

    init(
        sliceDurationMs: Int,
        bytesProcessed: Int,
        shardsCompleted: Int,
        deviceClass: String,
        extras: [String: String] = [:]
    ) {
        self.sliceDurationMs = sliceDurationMs
        self.bytesProcessed = bytesProcessed
        self.shardsCompleted = shardsCompleted
        self.deviceClass = deviceClass
        self.extras = extras
    }

    enum CodingKeys: String, CodingKey {
        case sliceDurationMs = "slice_duration_ms"
        case bytesProcessed = "bytes_processed"
        case shardsCompleted = "shards_completed"
        case deviceClass = "device_class"
    }

    // Custom encoding so `extras` are siblings of the top-level keys,
    // not a nested `"extras": {...}` object. Consumers of the metadata
    // column look keys up by name; a flat shape keeps their access
    // patterns stable.
    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: StringKey.self)
        try container.encode(sliceDurationMs, forKey: StringKey("slice_duration_ms"))
        try container.encode(bytesProcessed, forKey: StringKey("bytes_processed"))
        try container.encode(shardsCompleted, forKey: StringKey("shards_completed"))
        try container.encode(deviceClass, forKey: StringKey("device_class"))
        for (k, v) in extras {
            // Reserved keys can't be overwritten — skip any collision so
            // the structural guarantee holds.
            if Self.reservedKeys.contains(k) { continue }
            try container.encode(v, forKey: StringKey(k))
        }
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: StringKey.self)
        self.sliceDurationMs = try container.decode(Int.self, forKey: StringKey("slice_duration_ms"))
        self.bytesProcessed = try container.decode(Int.self, forKey: StringKey("bytes_processed"))
        self.shardsCompleted = try container.decode(Int.self, forKey: StringKey("shards_completed"))
        self.deviceClass = try container.decode(String.self, forKey: StringKey("device_class"))
        var extras: [String: String] = [:]
        for key in container.allKeys where !Self.reservedKeys.contains(key.stringValue) {
            if let v = try? container.decode(String.self, forKey: key) {
                extras[key.stringValue] = v
            }
        }
        self.extras = extras
    }

    private static let reservedKeys: Set<String> = [
        "slice_duration_ms",
        "bytes_processed",
        "shards_completed",
        "device_class",
    ]

    /// Encode to the JSON string stored on `work_journal.metadata`.
    /// Returns `"{}"` on (never-expected) encode failure so the column
    /// always holds valid JSON.
    func encodeJSON() -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        guard
            let data = try? encoder.encode(self),
            let s = String(data: data, encoding: .utf8)
        else {
            return "{}"
        }
        return s
    }

    /// Dynamic-string coding key. `Int?` returning nil keeps arbitrary
    /// keys flat in the encoded object.
    private struct StringKey: CodingKey {
        let stringValue: String
        init(_ s: String) { self.stringValue = s }
        init?(stringValue: String) { self.stringValue = stringValue }
        var intValue: Int? { nil }
        init?(intValue: Int) { nil }
    }
}

// MARK: - SliceCounters

/// In-memory aggregate counters keyed by device class. Flushed into the
/// `metadata` JSON at WorkJournal append time (via
/// ``snapshot(deviceClass:)``) — there is no separate event channel. On
/// relaunch the counters reset to zero; WorkJournal rows are the durable
/// system of record.
actor SliceCounters {
    struct Snapshot: Sendable, Equatable {
        let slicesStarted: Int
        let slicesCompleted: Int
        let slicesPaused: [InternalMissCause: Int]
        let slicesFailed: [InternalMissCause: Int]
    }

    /// Per-device-class counts. Keyed by `DeviceClass.rawValue` so
    /// consumers that parse the snapshot back through JSON don't have
    /// to reach into the enum.
    private var perDevice: [String: PerDeviceCounts] = [:]

    private struct PerDeviceCounts {
        var slicesStarted: Int = 0
        var slicesCompleted: Int = 0
        var slicesPaused: [InternalMissCause: Int] = [:]
        var slicesFailed: [InternalMissCause: Int] = [:]
    }

    func incrementStarted(deviceClass: DeviceClass) {
        perDevice[deviceClass.rawValue, default: .init()].slicesStarted += 1
    }

    func incrementCompleted(deviceClass: DeviceClass) {
        perDevice[deviceClass.rawValue, default: .init()].slicesCompleted += 1
    }

    func incrementPaused(deviceClass: DeviceClass, cause: InternalMissCause) {
        perDevice[deviceClass.rawValue, default: .init()]
            .slicesPaused[cause, default: 0] += 1
    }

    func incrementFailed(deviceClass: DeviceClass, cause: InternalMissCause) {
        perDevice[deviceClass.rawValue, default: .init()]
            .slicesFailed[cause, default: 0] += 1
    }

    func snapshot(deviceClass: DeviceClass) -> Snapshot {
        let entry = perDevice[deviceClass.rawValue] ?? .init()
        return Snapshot(
            slicesStarted: entry.slicesStarted,
            slicesCompleted: entry.slicesCompleted,
            slicesPaused: entry.slicesPaused,
            slicesFailed: entry.slicesFailed
        )
    }

    /// Reset counters. Test-only — production never clears counters;
    /// process restart is the natural reset boundary.
    func reset() {
        perDevice.removeAll()
    }
}

// MARK: - CauseEmissionRegistry

/// Process-wide registry of `InternalMissCause` variants that have a
/// live production emission path. The enum-exhaustiveness test
/// (`SliceCompletionInstrumentationTests.everyPhase1CauseHasEmitter`)
/// asserts every Phase-1 emitting variant is present.
///
/// Registration happens at module init via `Self.declareEmitters()` —
/// called once from a non-lazy static context so the registry populates
/// before any test runs. New emission sites add themselves to
/// `declareEmitters()` alongside the call-site tag.
///
/// The registry is intentionally lock-backed (not an actor) so
/// synchronous sites (`CapabilitySnapshot` access, `RuntimeRecord` hooks)
/// can register without hopping actors.
enum CauseEmissionRegistry {
    private static let state = OSAllocatedUnfairLock<State>(
        initialState: State()
    )

    private struct State {
        var emitters: [InternalMissCause: Set<String>] = [:]
        /// The subset of `emitters` whose call site is actually wired
        /// today (vs declared as a Phase 1.5 placeholder).
        var live: [InternalMissCause: Set<String>] = [:]
    }

    /// Record that `cause` has an active production emitter at `tag`.
    /// Idempotent — repeated calls with the same `(cause, tag)` are no-ops.
    ///
    /// Prefer `declareLive(...)` or `declarePlanned(...)` directly;
    /// callers that invoke `declare(...)` are treated as live so
    /// external emission sites don't need to understand the distinction.
    static func declare(cause: InternalMissCause, tag: String) {
        declareLive(cause: cause, tag: tag)
    }

    /// Record a LIVE emitter: production code at `tag` already writes
    /// `cause` to `work_journal.cause`.
    static func declareLive(cause: InternalMissCause, tag: String) {
        state.withLock { s in
            _ = s.emitters[cause, default: []].insert(tag)
            _ = s.live[cause, default: []].insert(tag)
        }
    }

    /// Record a PLANNED emitter: production code at `tag` is the
    /// documented site where this cause will be wired in playhead-dfem.
    /// The cause is declared for exhaustiveness but excluded from the
    /// live-emitter query.
    static func declarePlanned(cause: InternalMissCause, tag: String) {
        state.withLock { s in
            _ = s.emitters[cause, default: []].insert(tag)
        }
    }

    /// Whether `cause` has at least one declared production emitter
    /// (live OR planned).
    static func isDeclared(cause: InternalMissCause) -> Bool {
        state.withLock { s in !(s.emitters[cause]?.isEmpty ?? true) }
    }

    /// Whether `cause` has at least one LIVE production emitter
    /// (planned-only declarations do not count).
    static func isLiveEmitter(cause: InternalMissCause) -> Bool {
        state.withLock { s in !(s.live[cause]?.isEmpty ?? true) }
    }

    /// Return the tag set for `cause` (read-only snapshot). Includes
    /// both live and planned declarations.
    static func tags(for cause: InternalMissCause) -> Set<String> {
        state.withLock { s in s.emitters[cause] ?? [] }
    }

    /// Return ONLY the live-emitter tags for `cause`.
    static func liveTags(for cause: InternalMissCause) -> Set<String> {
        state.withLock { s in s.live[cause] ?? [] }
    }

    /// Every Phase-1 emitting cause. Non-emitters (`noRuntimeGrant`,
    /// `modelTemporarilyUnavailable`, `unsupportedEpisodeLanguage`) are
    /// absent by design — they are eligibility-layer concerns with no
    /// live slice emission this phase.
    static let phase1EmittingCauses: Set<InternalMissCause> = [
        .taskExpired,
        .thermal,
        .lowPowerMode,
        .batteryLowUnplugged,
        .noNetwork,
        .wifiRequired,
        .mediaCap,
        .analysisCap,
        .userPreempted,
        .userCancelled,
        .asrFailed,
        .pipelineError,
        .appForceQuitRequiresRelaunch,
    ]

    /// Causes NOT emitted in Phase 1. Present as a named set so the
    /// exhaustiveness test can assert the complement explicitly.
    static let phase1NonEmittingCauses: Set<InternalMissCause> = [
        .noRuntimeGrant,
        .modelTemporarilyUnavailable,
        .unsupportedEpisodeLanguage,
    ]

    /// Bootstrap every known production emitter. Invoked from
    /// ``bootstrap()`` which is called at app launch from
    /// `PlayheadAppDelegate` AND eagerly by tests via
    /// `SliceCompletionInstrumentation.ensureBootstrapped()`.
    ///
    /// Emitters are split into two groups:
    ///
    ///   * **Live**: production code at the listed site already writes
    ///     the cause to `work_journal.cause` via
    ///     `WorkJournalRecording.recordFailed`/`recordPreempted` or
    ///     `AnalysisStore.releaseEpisodeLease(... cause:)`.
    ///   * **Planned**: production code at the listed site does NOT yet
    ///     write the cause; the tag documents the call graph location
    ///     for the Phase 1.5 wire-up bead (playhead-dfem). Declaring them
    ///     here keeps the exhaustiveness property mechanical: the test
    ///     asserts "every Phase-1 emitting cause has at least one known
    ///     call-site"; the second assertion
    ///     (``isLiveEmitter(cause:)``) narrows that to "at least one
    ///     LIVE writer" and is currently relaxed for the planned set.
    ///
    /// When you add a new emission site, add a `declareLive(...)` call
    /// here (and remove the corresponding `declarePlanned(...)` if
    /// upgrading a stub to a live writer).
    static func declareEmitters() {
        // MARK: Live — already wired by predecessor beads.

        // no_network, wifi_required, pipeline_error — DownloadManager
        // maps URLError → InternalMissCause and writes via
        // `EpisodeDownloadDelegate.urlSession(_:task:didCompleteWithError:)`.
        declareLive(cause: .noNetwork, tag: "DownloadManager.EpisodeDownloadDelegate.urlSession.didCompleteWithError")
        declareLive(cause: .wifiRequired, tag: "DownloadManager.EpisodeDownloadDelegate.urlSession.didCompleteWithError")
        declareLive(cause: .pipelineError, tag: "DownloadManager.EpisodeDownloadDelegate.urlSession.didFinishDownloadingTo")
        declareLive(cause: .pipelineError, tag: "ForceQuitResumeScan.scanForSuspendedTransfers.corrupted")

        // appForceQuitRequiresRelaunch — ForceQuitResumeScan emits
        // `preempted` with this cause on cold-start scan.
        declareLive(cause: .appForceQuitRequiresRelaunch, tag: "ForceQuitResumeScan.scanForSuspendedTransfers.resumable")

        // userPreempted — LanePreemptionCoordinator flips the signal
        // with cause=.userPreempted; AnalysisJobRunner returns the
        // preempted outcome and the coordinator releases the episode
        // lease with the cause.
        declareLive(cause: .userPreempted, tag: "LanePreemptionCoordinator.PreemptionSignal.request")
        declareLive(cause: .userPreempted, tag: "AnalysisJobRunner.run.preempted")

        // task_expired — BGProcessingTask expirationHandler fires when
        // iOS reclaims the window; BackgroundProcessingService invokes
        // `AnalysisWorkScheduler.cancelCurrentJob(cause: .taskExpired)`
        // which threads the cause down to the recorder so WorkJournal
        // gets `cause = task_expired`.
        declareLive(cause: .taskExpired, tag: "BackgroundProcessingService.handleExpiredProcessingTask")
        declareLive(cause: .taskExpired, tag: "AnalysisWorkScheduler.cancelCurrentJob.taskExpired")

        // userCancelled — the explicit user-cancel entry. The scheduler
        // exposes `cancelCurrentJob(cause: .userCancelled)` and forwards
        // the cause through the injected WorkJournal recorder. The UI
        // call site is wired in playhead-dfem; the scheduler-side path
        // is live today and is exercised by unit tests that pass
        // `.userCancelled` directly.
        declareLive(cause: .userCancelled, tag: "AnalysisWorkScheduler.cancelCurrentJob.userCancelled")

        // MARK: Planned — blocked by admission-layer refactor (playhead-dfem).
        //
        // Each entry below names a production site where the emission
        // MUST land once dfem completes the admission-layer refactor.
        // The exhaustiveness test sees these as "declared" (so the test
        // infrastructure is ready when they go live) but they are
        // excluded from `liveEmitters` so a regression auditor can see
        // at a glance which causes still need writers.

        // thermal — AnalysisWorkScheduler's per-loop QualityProfile
        // evaluation blocks lanes when thermal demotes the profile; the
        // emission must be recorded on any currently-running slice that
        // terminates at the pause gate. Blocked by dfem (admission-layer
        // hook that surfaces the thermal gate decision to the running
        // slice).
        declarePlanned(cause: .thermal, tag: "AnalysisWorkScheduler.admissionDeferred.thermal")
        declarePlanned(cause: .thermal, tag: "AnalysisJobRunner.checkStopConditions.pausedForThermal")

        // low_power_mode — emitted independently of thermal (LPM with
        // thermal .nominal); same admission path as thermal. Blocked by
        // dfem.
        declarePlanned(cause: .lowPowerMode, tag: "AnalysisWorkScheduler.admissionDeferred.lowPowerMode")

        // battery_low_unplugged — battery guard rail at admission time.
        // Blocked by dfem.
        declarePlanned(cause: .batteryLowUnplugged, tag: "AnalysisWorkScheduler.admissionDeferred.batteryLowUnplugged")

        // media_cap / analysis_cap — StorageBudget admission rejections.
        // DownloadManager consults StorageBudget before committing a
        // transfer; on rejection it must emit the corresponding cause.
        // Blocked by dfem (StorageBudget admission hook does not yet
        // surface a typed rejection reason to DownloadManager).
        //
        // Canonical future site is `DownloadManager.performDownload` —
        // the interior pipeline that every entry point
        // (`progressiveDownload`, `streamingDownload`,
        // `backgroundDownload`) funnels through, and the natural place
        // to branch on a `StorageBudget.admit(...)` rejection before
        // URLSession work starts. Tag strings name that method so
        // planned/live diffs match reality once the hook lands.
        // Future site; tracked in playhead-dfem.
        declarePlanned(cause: .mediaCap, tag: "DownloadManager.performDownload.storageBudgetRejected.media")
        declarePlanned(cause: .analysisCap, tag: "DownloadManager.performDownload.storageBudgetRejected.analysis")

        // asr_failed — TranscriptEngineService throws on model failure
        // or returns no segments; AnalysisJobRunner.run(...) surfaces
        // this as `.failed("transcription:zeroCoverage")` at the
        // `.preempted`/`.failed` outcome boundary. Upgraded to LIVE in
        // playhead-5uvz.7 (Gap-9): the runner now writes a structured
        // `work_journal` row directly via
        // `AnalysisStore.appendWorkJournalEntry(_:)` from the zero-
        // coverage failure branch, so the cause has a real production
        // emitter without needing the dfem runner-recorder injection.
        declareLive(cause: .asrFailed, tag: "AnalysisJobRunner.run.transcriptionTimeout")

        // pipeline_error catch-all in the runner. Already declared live
        // for the DownloadManager site above; the runner hook is
        // planned (AnalysisJobRunner does not yet funnel through the
        // episode-level lease release with a cause). Blocked by dfem.
        declarePlanned(cause: .pipelineError, tag: "AnalysisJobRunner.run.catchAll")
    }

    // MARK: - Test helpers

    /// Causes that are LIVE in Phase 1 — the subset of
    /// `phase1EmittingCauses` whose production writer already exists.
    /// Stays in sync with the `declareLive(...)` calls in
    /// `declareEmitters()`. The exhaustiveness test uses this to assert
    /// "every live cause has a matching `isLiveEmitter` registration".
    static let phase1LiveCauses: Set<InternalMissCause> = [
        .noNetwork,
        .wifiRequired,
        .pipelineError,
        .userPreempted,
        .appForceQuitRequiresRelaunch,
        .taskExpired,
        .userCancelled,
        // playhead-5uvz.7: AnalysisJobRunner.run writes a structured
        // `failed` row directly to `work_journal` from the zero-coverage
        // transcription branch (cause = .asrFailed).
        .asrFailed,
    ]

    #if DEBUG
    /// Reset registry state for tests that need isolation across cases.
    /// Production code must never call this.
    static func resetForTesting() {
        state.withLock { s in
            s.emitters.removeAll()
            s.live.removeAll()
        }
    }

    /// Snapshot every `(cause, tag)` pair currently registered (all
    /// declarations, live + planned).
    static func snapshotForTesting() -> [InternalMissCause: Set<String>] {
        state.withLock { s in s.emitters }
    }

    /// Snapshot every LIVE `(cause, tag)` pair currently registered.
    static func liveSnapshotForTesting() -> [InternalMissCause: Set<String>] {
        state.withLock { s in s.live }
    }
    #endif
}

// MARK: - Bootstrap

/// Bootstrap facade for `playhead-1nl6` instrumentation. Call
/// ``bootstrap()`` at app launch (from `PlayheadAppDelegate`) AND at
/// test-suite startup; both paths are idempotent.
///
/// Also owns the process-wide ``counters`` actor that every live
/// emission site increments on a preempt / fail. Tests can reach the
/// counters via ``counters`` directly; production call sites invoke
/// the convenience helpers (``recordPreempted(...)`` /
/// ``recordFailed(...)``) which fold metadata JSON construction, the
/// counter increment, and a `CauseEmissionRegistry` live tag into one
/// call so new sites can't skip a step.
enum SliceCompletionInstrumentation {
    private static let bootstrapped = OSAllocatedUnfairLock<Bool>(initialState: false)

    /// Process-wide counters. Shared — every live emission site funnels
    /// through this actor. Reset only happens on process restart (or via
    /// ``SliceCounters/reset()`` in tests).
    static let counters = SliceCounters()

    /// Populate `CauseEmissionRegistry` with every known production
    /// emitter. Idempotent.
    static func bootstrap() {
        bootstrapped.withLock { done in
            if done { return }
            done = true
            CauseEmissionRegistry.declareEmitters()
        }
    }

    /// Test-time bootstrap that resets + re-registers the registry so
    /// test isolation holds regardless of prior state.
    #if DEBUG
    static func ensureBootstrapped() {
        bootstrapped.withLock { done in
            if !done {
                CauseEmissionRegistry.declareEmitters()
                done = true
            }
        }
    }
    #endif

    // MARK: - Emission convenience

    /// Build a `SliceMetadata` blob for a terminal (preempted/failed)
    /// event. Accepts the four contract fields and (optionally) a
    /// dictionary of flat extras. Call sites that genuinely cannot
    /// determine a field (e.g. `bytesProcessed` on a force-quit resume
    /// scan that never touched the byte counter) pass `0` and name the
    /// reason in a comment at the call site — the spec says every
    /// acquired→terminal transition emits metadata, so skipping the
    /// blob is not an option.
    static func buildMetadata(
        sliceDurationMs: Int,
        bytesProcessed: Int,
        shardsCompleted: Int,
        deviceClass: DeviceClass,
        extras: [String: String] = [:]
    ) -> SliceMetadata {
        SliceMetadata(
            sliceDurationMs: sliceDurationMs,
            bytesProcessed: bytesProcessed,
            shardsCompleted: shardsCompleted,
            deviceClass: deviceClass.rawValue,
            extras: extras
        )
    }

    /// Record a paused (preempted) terminal event: increments the
    /// `slicesPaused[cause]` counter for `deviceClass` and returns the
    /// encoded metadata JSON blob the caller should pass to
    /// `WorkJournalRecording.recordPreempted(...)`.
    @discardableResult
    static func recordPaused(
        cause: InternalMissCause,
        deviceClass: DeviceClass,
        sliceDurationMs: Int,
        bytesProcessed: Int,
        shardsCompleted: Int,
        extras: [String: String] = [:]
    ) async -> SliceMetadata {
        let metadata = buildMetadata(
            sliceDurationMs: sliceDurationMs,
            bytesProcessed: bytesProcessed,
            shardsCompleted: shardsCompleted,
            deviceClass: deviceClass,
            extras: extras
        )
        await counters.incrementPaused(deviceClass: deviceClass, cause: cause)
        return metadata
    }

    /// Record a failed terminal event: increments the
    /// `slicesFailed[cause]` counter for `deviceClass` and returns the
    /// encoded metadata JSON blob the caller should pass to
    /// `WorkJournalRecording.recordFailed(...)`.
    @discardableResult
    static func recordFailed(
        cause: InternalMissCause,
        deviceClass: DeviceClass,
        sliceDurationMs: Int,
        bytesProcessed: Int,
        shardsCompleted: Int,
        extras: [String: String] = [:]
    ) async -> SliceMetadata {
        let metadata = buildMetadata(
            sliceDurationMs: sliceDurationMs,
            bytesProcessed: bytesProcessed,
            shardsCompleted: shardsCompleted,
            deviceClass: deviceClass,
            extras: extras
        )
        await counters.incrementFailed(deviceClass: deviceClass, cause: cause)
        return metadata
    }
}

// MARK: - Resolve-and-record convenience

extension CauseAttributionPolicy {
    /// Convenience wrapper around ``resolve(causes:context:)`` for
    /// emission call sites that collect multiple live causes and want
    /// the primary back to write into `work_journal.cause`. Returns
    /// `nil` when `causes` is empty.
    ///
    /// Callers that already have a single known cause should not funnel
    /// through here — pass the cause directly to the WorkJournal
    /// recorder. This helper exists specifically for the admission /
    /// scheduler path where several causes can coincide.
    static func primaryCause(
        among causes: [InternalMissCause],
        context: CauseAttributionContext
    ) -> InternalMissCause? {
        selectPrimary(causes: causes, context: context)?.primary
    }
}
