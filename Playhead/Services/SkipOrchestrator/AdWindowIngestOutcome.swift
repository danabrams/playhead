// AdWindowIngestOutcome.swift
// playhead-isp5 — a NAME for every way a persisted ad window does or does not
// reach the live session.
//
// WHY THIS EXISTS. On 2026-08-01 a day-0 rediff minted four mark-only windows
// for episode D9B513CD, persisted all four at confidence 1.00, and handed them
// to `SkipOrchestrator.ingestPersistedAdWindows` with the playhead sitting
// inside the first one. No banner appeared. Two investigations
// ([[playhead-djl0]], then this bead) could not say WHY from the evidence the
// app left behind, because the whole delivery path reported itself through
// `os_log` — a channel the device pull does not read — and because
// `receiveAdWindows` has eighteen terminal dispositions that were, from the
// outside, indistinguishable from each other and from "never called".
//
// The remedy follows the pattern playhead-djl0 (#317) established and
// playhead-v7q6 (#316) settled: the DURABLE ROW is the audit trail, not the
// diagnostic log. Every disposition here is counted for the process lifetime
// and summarised into one `ad_window_ingest_census` entry per delivery in the
// `SurfaceStatusInvariantLogger` session file that ships in the diagnostics
// bundle. One device pull now answers "where did the windows go?".
//
// A note on scope: the per-window cases are stamped by `receiveAdWindows`,
// which is shared by the cross-launch preload, the mid-session ingest, the
// hot-path push and the final-pass backfill push. The counters are therefore
// process-wide across all four producers, deliberately — a drop cause does not
// become a different fact because a different producer supplied the row. The
// per-DELIVERY attribution comes from the census, which reads back only the
// ids the door itself forwarded.

import Foundation

/// Which door a persisted-window delivery came through. Both doors run the
/// same admission rule (`forwardPersistedAdWindows`); they differ only in when
/// they fire, and the census records which one so a field log can tell an
/// episode-start preload apart from a mid-session day-0 mint.
enum AdWindowIngestDoor: String, Sendable, Hashable, CaseIterable {
    /// `beginEpisode`'s cross-launch preload — fires once per episode start.
    case crossLaunchPreload = "cross_launch_preload"
    /// playhead-96ot's `ingestPersistedAdWindows` — fires when a mint lands
    /// during playback.
    case midSessionIngest = "mid_session_ingest"
}

/// One terminal disposition of a persisted ad window, named.
///
/// The `door*` cases describe a whole delivery CALL (there is at most one per
/// call, and it is recorded only when the call never reached
/// `receiveAdWindows`). Every other case describes ONE window.
///
/// Raw values are the strings that appear in the diagnostics session file and
/// are the stable audit key — rename the case freely, never the raw value.
enum AdWindowIngestOutcome: String, Sendable, Hashable, CaseIterable {

    // MARK: - Door outcomes (at most one per delivery call)

    /// The minted asset is not the one playing, so nothing was forwarded. Not
    /// a loss: the rows are durable and the next `beginEpisode` preloads them.
    /// This is the outcome playhead-isp5 was filed to be able to rule out.
    case doorDroppedNotPlaying = "ingest_door_dropped_not_playing"

    /// `fetchAdWindows` threw. Nothing was forwarded and nothing is known
    /// about the rows.
    case doorDroppedStoreReadFailed = "ingest_door_dropped_store_read_failed"

    /// The episode was replaced while the store read was in flight, so the
    /// rows belong to a session that no longer exists.
    case doorDroppedEpisodeReplaced = "ingest_door_dropped_episode_replaced"

    /// The store read succeeded and NO row passed the admission filter
    /// (`preloadAdmissibleWindows`) — below the confidence floor, degenerate
    /// geometry, or a terminal decision state. Distinct from "no rows at all",
    /// which is the same outcome with `forwarded == 0`.
    case doorDroppedNoAdmissibleRows = "ingest_door_dropped_no_admissible_rows"

    // MARK: - Per-window outcomes: the window reached a tier

    /// The window entered the managed (auto-skip) tier.
    case admittedManaged = "ingest_admitted_managed"

    /// The window entered the suggest tier and is ARMED — it will banner when
    /// the playhead enters its span (playhead-d3g0). This is the success case
    /// the 2026-08-01 field episode expected and did not get.
    case armedSuggest = "ingest_armed_suggest"

    /// An exact replay of an already-`.applied` producer revision: the durable
    /// receipt and its cue are kept, and nothing new is presented.
    case retainedAppliedReceipt = "ingest_retained_applied_receipt"

    // MARK: - Per-window outcomes: the window was dropped

    /// `receiveAdWindows` ran with no active episode.
    case droppedNoActiveEpisode = "ingest_dropped_no_active_episode"

    /// The row's `analysisAssetId` is not the playing asset.
    case droppedForeignAsset = "ingest_dropped_foreign_asset"

    /// A newer mutation for the same window id superseded this one.
    case droppedStaleProducerRevision = "ingest_dropped_stale_producer_revision"

    /// A replay of a revision already recorded as producer-terminal.
    case droppedTerminalProducerReplay = "ingest_dropped_terminal_producer_replay"

    /// The episode was replaced while catalog validation was in flight.
    case droppedEpisodeReplaced = "ingest_dropped_episode_replaced"

    /// Non-finite times, a non-canonical id, an out-of-range confidence, or a
    /// non-positive span.
    case droppedInvalidMaterial = "ingest_dropped_invalid_material"

    /// The persisted `decisionState` string does not decode.
    case droppedMalformedDecisionState = "ingest_dropped_malformed_decision_state"

    /// The persisted `eligibilityGate` string is neither nil, nor `autoSkip`,
    /// nor a known `SkipEligibilityGate` raw value.
    case droppedMalformedEligibilityGate = "ingest_dropped_malformed_eligibility_gate"

    /// The user already answered Yes or No to this window id.
    case droppedUserResolvedSuggestion = "ingest_dropped_user_resolved_suggestion"

    /// The user chose "Listen" for this producer id.
    case droppedUserReverted = "ingest_dropped_user_reverted"

    /// The producer stamped the row `.reverted` or `.suppressed`.
    case droppedProducerTerminalState = "ingest_dropped_producer_terminal_state"

    /// playhead-xr3t's `InventorySanityFilter` rejected the span. The census
    /// carries the rejection REASON (`tooShort` / `tooEarly` / `tooLate` /
    /// `overlapsDeclaredChapter`), because those four are four different bugs.
    ///
    /// `tooEarly` in particular fires on any span starting inside the first
    /// three seconds of the episode — which is every pre-roll.
    case droppedInventorySanity = "ingest_dropped_inventory_sanity"

    /// A recognised non-`.eligible` gate other than `.markOnly` (the blocked-*
    /// and capped cases) — never admitted to the auto-skip tier.
    case droppedBlockedGate = "ingest_dropped_blocked_gate"

    /// A mark-only window whose id is already in `banneredWindowIds`, so the
    /// suggest tier deliberately does not ask again.
    case droppedAlreadyBannered = "ingest_dropped_already_bannered"

    /// A mark-only window whose exact revision the suggest tier had already
    /// presented and the listener had already acknowledged, so
    /// `registerSuggestedWindow` kept it registered without re-arming. Not a
    /// loss and not a delivery: the question was asked once, which is the
    /// once-per-window-per-episode guarantee working.
    case suggestReplayNotRearmed = "ingest_suggest_replay_not_rearmed"

    /// The window is held while a user answer for the same id resolves; the
    /// update is buffered rather than lost.
    case bufferedProvisionalResolution = "ingest_buffered_provisional_resolution"

    // MARK: - Classification

    /// True for the outcomes where the window ended up somewhere a listener
    /// can eventually see. Used by the census to answer, in one number, the
    /// question the field case could not: did ANY of what we delivered land?
    var isDelivered: Bool {
        switch self {
        case .admittedManaged, .armedSuggest, .retainedAppliedReceipt:
            return true
        case .doorDroppedNotPlaying, .doorDroppedStoreReadFailed,
             .doorDroppedEpisodeReplaced, .doorDroppedNoAdmissibleRows,
             .droppedNoActiveEpisode, .droppedForeignAsset,
             .droppedStaleProducerRevision, .droppedTerminalProducerReplay,
             .droppedEpisodeReplaced, .droppedInvalidMaterial,
             .droppedMalformedDecisionState, .droppedMalformedEligibilityGate,
             .droppedUserResolvedSuggestion, .droppedUserReverted,
             .droppedProducerTerminalState, .droppedInventorySanity,
             .droppedBlockedGate, .droppedAlreadyBannered,
             .suggestReplayNotRearmed, .bufferedProvisionalResolution:
            return false
        }
    }

    /// True for the four whole-call outcomes. An exhaustive switch rather than
    /// a raw-value prefix test so a new case forces an author decision.
    var isDoorOutcome: Bool {
        switch self {
        case .doorDroppedNotPlaying, .doorDroppedStoreReadFailed,
             .doorDroppedEpisodeReplaced, .doorDroppedNoAdmissibleRows:
            return true
        case .admittedManaged, .armedSuggest, .retainedAppliedReceipt,
             .droppedNoActiveEpisode, .droppedForeignAsset,
             .droppedStaleProducerRevision, .droppedTerminalProducerReplay,
             .droppedEpisodeReplaced, .droppedInvalidMaterial,
             .droppedMalformedDecisionState, .droppedMalformedEligibilityGate,
             .droppedUserResolvedSuggestion, .droppedUserReverted,
             .droppedProducerTerminalState, .droppedInventorySanity,
             .droppedBlockedGate, .droppedAlreadyBannered,
             .suggestReplayNotRearmed, .bufferedProvisionalResolution:
            return false
        }
    }
}

/// One delivery's full accounting: what the door was asked to deliver, and
/// where each row ended up. Rendered into the `ad_window_ingest_census` audit
/// row as stable `key=value` pairs so a device pull can be grepped rather than
/// parsed.
struct AdWindowIngestCensus: Sendable, Hashable {

    /// Which door produced this delivery.
    let door: AdWindowIngestDoor

    /// The asset the delivery was for. A UUID, never user content.
    let analysisAssetId: String

    /// How many rows the door handed to `receiveAdWindows`. Zero when a door
    /// outcome short-circuited the call.
    let forwarded: Int

    /// Terminal disposition counts, keyed by outcome.
    let counts: [AdWindowIngestOutcome: Int]

    /// Free-form sub-cause tallies for the outcomes that carry one — today
    /// only `droppedInventorySanity`, whose four rejection reasons are four
    /// unrelated bugs. Keyed `"<outcome rawValue>:<detail>"`.
    let details: [String: Int]

    /// How many forwarded rows reached a tier a listener can eventually see.
    var deliveredCount: Int {
        counts.reduce(0) { $0 + ($1.key.isDelivered ? $1.value : 0) }
    }

    /// The audit row's body. Sorted by raw value so two identical deliveries
    /// render byte-identically and a diff between sessions is meaningful.
    var auditDescription: String {
        var parts = [
            "door=\(door.rawValue)",
            "asset=\(analysisAssetId)",
            "forwarded=\(forwarded)",
            "delivered=\(deliveredCount)",
        ]
        for (outcome, count) in counts.sorted(by: {
            $0.key.rawValue < $1.key.rawValue
        }) where count > 0 {
            parts.append("\(outcome.rawValue)=\(count)")
        }
        for (detail, count) in details.sorted(by: { $0.key < $1.key })
        where count > 0 {
            parts.append("\(detail)=\(count)")
        }
        return parts.joined(separator: " ")
    }
}
