// LiveActivityCopy.swift
// playhead-44h1: authoritative template strings for the system Live
// Activity surface that reports on in-flight downloads and analysis.
//
// The WidgetKit consumer for these strings is intentionally deferred —
// this provider is the single source of truth for the copy itself, and
// a snapshot test against the provider stands in for a full rendered
// snapshot until the WidgetKit target lands in a later bead.
//
// Scope:
//   - This file authors provisional copy for the three Phase 1 Live
//     Activity states: `downloading`, `analyzing`, `paused`.
//   - The final `SurfaceReason → user-visible text` table is playhead-
//     dfem's job (Phase 1.5). For `paused`, this file uses a
//     provisional inline mapping so the activity renders something
//     useful before dfem lands. The mapping is deliberately minimal —
//     plain-English phrases keyed off the raw `SurfaceReason` enum, no
//     internal engine vocabulary.
//
// Lint contract (bead spec, carried over from playhead-r835):
//   - No copy string may contain the bare words "Now", "Soon", or
//     "Background". Those names are scheduler-internal lane labels.
//     The `SchedulerLaneUILintTests` grep lint enforces the
//     `SchedulerLane` type leakage; this file adds its own assertion
//     in the snapshot tests to guard the three user-visible lane
//     labels.
//   - The raw `InternalMissCause` name is never surfaced. Mapping
//     goes `InternalMissCause → SurfaceReason → copy` (dfem will
//     author the full table; this bead only consumes the
//     `SurfaceReason` step).
//
// Byte formatting uses `ByteCountFormatter` with the binary style so
// the displayed magnitudes match what iOS uses elsewhere in Settings /
// Files / App Store (e.g. "1.2 GB"). The spec example "X GB / Y GB"
// is satisfied by two separate formats joined by " / " to keep the
// formatter's unit suffix attached to each number.

import Foundation

// MARK: - State descriptors

/// Describes the currently-active download surface. `queuedCount` is
/// the user-visible episode count ("Downloading N episodes"); `N` is
/// typically the number of queued Now-lane downloads plus the one in
/// flight. Bytes are the aggregate across all active transfers so the
/// activity shows a single total, not a per-episode breakdown (per
/// spec).
struct LiveActivityDownloadingState: Sendable, Equatable {
    let queuedCount: Int
    let totalBytesWritten: Int64
    let totalBytesExpectedToWrite: Int64
}

/// Describes the currently-active analysis job. `episodeDurationSec`
/// is the full episode length used to compute a `totalShardsEstimate =
/// ceil(duration / nominalShardDuration)`; `shardsCompleted` is the
/// most recent value persisted to the WorkJournal `checkpointed`
/// metadata key `shards_completed` (playhead-1nl6).
///
/// `shardsCompleted` is `nil` when the WorkJournal key is absent — in
/// particular, when running without 1nl6 landed. This bead's formula
/// tolerates that absence by falling back to 0 and still producing a
/// best-effort ETA.
struct LiveActivityAnalyzingState: Sendable, Equatable {
    let episodeDurationSec: Double
    let shardsCompleted: Int?
    let nominalShardDurationSec: Double
    let avgShardDurationMs: Int
    let queuedRemaining: Int

    /// Convenience ctor for callers that want the "no job running,
    /// N to go" state. Passing `episodeDurationSec <= 0` routes
    /// through `LiveActivityCopy.analyzingText(...)` to the
    /// `"Queued · N to go"` template — the shard fields are ignored
    /// in that branch.
    static func queuedOnly(queuedRemaining: Int) -> LiveActivityAnalyzingState {
        LiveActivityAnalyzingState(
            episodeDurationSec: 0,
            shardsCompleted: nil,
            nominalShardDurationSec: 20,
            avgShardDurationMs: 0,
            queuedRemaining: queuedRemaining
        )
    }
}

/// Describes a currently-paused job. `reason` is the `SurfaceReason`
/// returned by `CauseAttributionPolicy.attribution(...)` from the
/// job's last release event. The provisional mapping below translates
/// it to plain-English copy.
struct LiveActivityPausedState: Sendable, Equatable {
    let reason: SurfaceReason
}

// MARK: - Provider

/// Authoritative Live Activity copy provider. Every template string
/// the system activity renders routes through one of the three
/// `*Text(for:)` entry points. The returned strings are
/// Equatable-comparable so snapshot tests can pin them against known
/// baselines without a WidgetKit target.
enum LiveActivityCopy {

    // MARK: - Downloading

    /// Format an aggregate-bytes download string.
    ///
    /// Template: `"Downloading N episodes · X GB / Y GB"`
    ///
    /// When `queuedCount == 1`, the pluralization collapses to
    /// `"Downloading 1 episode"` (bare-singular, no "1 episodes"
    /// typo). When the expected-total is unknown (`<= 0`), the byte
    /// suffix is omitted and the template becomes just
    /// `"Downloading N episode(s)"` — the counters are still
    /// informative even without a denominator.
    static func downloadingText(for state: LiveActivityDownloadingState) -> String {
        let episodesPhrase = state.queuedCount == 1
            ? "Downloading 1 episode"
            : "Downloading \(state.queuedCount) episodes"

        guard state.totalBytesExpectedToWrite > 0 else {
            return episodesPhrase
        }

        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useGB, .useMB, .useKB]
        formatter.countStyle = .binary
        formatter.includesUnit = true
        let written = formatter.string(fromByteCount: max(0, state.totalBytesWritten))
        let total = formatter.string(fromByteCount: state.totalBytesExpectedToWrite)
        return "\(episodesPhrase) · \(written) / \(total)"
    }

    // MARK: - Analyzing

    /// Fallback `avgShardDurationMs` when the device-class profile
    /// lookup misses (unknown device class). Matches the spec value.
    static let fallbackAvgShardDurationMs: Int = 4500

    /// Resolve the `avgShardDurationMs` value from a device-class
    /// profile lookup. Returns the profile's value when present and
    /// positive; falls back to ``fallbackAvgShardDurationMs`` (4500 ms)
    /// on `nil` or non-positive values.
    ///
    /// Kept separate from the `analyzingText(for:)` path so the
    /// lookup chain is independently testable with mocked profiles
    /// without constructing the full `LiveActivityAnalyzingState`.
    /// The production Live Activity plumbing (playhead-iwiy) routes
    /// its device-class lookup through this helper so the fallback
    /// rule is enforced in exactly one place.
    static func resolveAvgShardDurationMs(
        from profile: DeviceClassProfile?
    ) -> Int {
        guard let profile, profile.avgShardDurationMs > 0 else {
            return fallbackAvgShardDurationMs
        }
        return profile.avgShardDurationMs
    }

    /// Overload of ``analyzingText(for:)`` that takes a
    /// ``DeviceClassProfile?`` directly instead of a pre-resolved
    /// `avgShardDurationMs`. Thin wrapper over
    /// ``resolveAvgShardDurationMs(from:)`` + the Int-taking entry
    /// point — provided so callers that already hold a device-class
    /// profile do not have to round-trip through the resolver
    /// themselves.
    static func analyzingText(
        episodeDurationSec: Double,
        shardsCompleted: Int?,
        nominalShardDurationSec: Double,
        queuedRemaining: Int,
        deviceProfile: DeviceClassProfile?
    ) -> String {
        let avgMs = resolveAvgShardDurationMs(from: deviceProfile)
        let state = LiveActivityAnalyzingState(
            episodeDurationSec: episodeDurationSec,
            shardsCompleted: shardsCompleted,
            nominalShardDurationSec: nominalShardDurationSec,
            avgShardDurationMs: avgMs,
            queuedRemaining: queuedRemaining
        )
        return analyzingText(for: state)
    }

    /// Format the currently-running-analysis template. Per spec:
    ///
    /// - "Analyzing · ~M min remaining" where M is
    ///   `(remainingShards × avgShardDurationMs) / 60000`.
    /// - `remainingShards = totalShardsEstimate - shardsCompleted`.
    /// - `totalShardsEstimate = ceil(duration / nominalShardDuration)`.
    /// - If `shardsCompleted` is nil (1nl6 key missing), fall back to
    ///   0 so the formula still produces a best-effort estimate.
    /// - If `avgShardDurationMs <= 0`, substitute
    ///   `fallbackAvgShardDurationMs`.
    /// - If no running job (episodeDuration <= 0), return
    ///   `"Queued · N to go"` when queuedRemaining > 0, else
    ///   `"Queued"`.
    static func analyzingText(for state: LiveActivityAnalyzingState) -> String {
        if state.episodeDurationSec <= 0 {
            if state.queuedRemaining > 0 {
                return "Queued · \(state.queuedRemaining) to go"
            }
            return "Queued"
        }

        let nominal = max(1, state.nominalShardDurationSec)
        let totalShardsDouble = (state.episodeDurationSec / nominal).rounded(.up)
        let totalShardsEstimate = max(0, Int(totalShardsDouble))
        let completed = max(0, state.shardsCompleted ?? 0)
        let remainingShards = max(0, totalShardsEstimate - completed)

        let shardMs = state.avgShardDurationMs > 0
            ? state.avgShardDurationMs
            : fallbackAvgShardDurationMs
        let remainingMs = remainingShards * shardMs
        // Round up so "1.1 min" rounds to "~2 min remaining" — better
        // to slightly over-promise time than to show "~0 min" when
        // work is still pending. A remaining count of 0 renders as
        // "~0 min" intentionally to signal near-completion without
        // claiming done.
        let remainingMinutes: Int
        if remainingMs <= 0 {
            remainingMinutes = 0
        } else {
            remainingMinutes = max(1, Int((Double(remainingMs) / 60_000.0).rounded(.up)))
        }
        return "Analyzing · ~\(remainingMinutes) min remaining"
    }

    // MARK: - Paused

    /// Format a paused-state template. Per spec:
    /// `"Paused — <SurfaceReason text>"`. The `SurfaceReason → text`
    /// mapping is provisional (playhead-dfem authors the canonical
    /// table in Phase 1.5). This mapping covers every
    /// `SurfaceReason` case with a plain-English phrase so the
    /// activity never renders a raw enum name.
    static func pausedText(for state: LiveActivityPausedState) -> String {
        "Paused — \(provisionalReasonText(state.reason))"
    }

    /// Provisional `SurfaceReason → user-visible string` mapping.
    /// Phase 1.5 (playhead-dfem) replaces this with the canonical
    /// copy table; until then every case is covered so the activity
    /// never renders a raw enum name.
    static func provisionalReasonText(_ reason: SurfaceReason) -> String {
        switch reason {
        case .waitingForTime:
            return "waiting for a quiet moment"
        case .phoneIsHot:
            return "the phone is warm"
        case .powerLimited:
            return "power saver is on"
        case .waitingForNetwork:
            return "waiting for Wi-Fi"
        case .storageFull:
            return "storage is full"
        case .analysisUnavailable:
            return "analysis is unavailable on this device"
        case .resumeInApp:
            return "open the app to resume"
        case .cancelled:
            return "cancelled"
        case .couldntAnalyze:
            return "couldn't analyze this episode"
        }
    }
}
