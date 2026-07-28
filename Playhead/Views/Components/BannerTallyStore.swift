// BannerTallyStore.swift
// Per-episode banner-card tally (playhead-bfq7).
//
// The question this answers: "how many banner cards did that episode
// put in front of me?" Counting by hand during a listening session is
// unreliable — cards can fire back to back — and that count is the
// single number the playhead-zkcg decision rests on.
//
// ----- Two surfaces, because they answer different questions -----
//
//   1. LIVE (os_log). One breadcrumb per card carrying a running
//      per-episode index, the tier that produced it, and the span. A
//      Console.app filter then gives both a live count and a timeline,
//      and the index keeps fast consecutive cards individually
//      attributable.
//   2. AFTER THE FACT (diagnostics bundle). A per-episode row with the
//      total and the per-tier split, so a session is auditable from an
//      exported bundle even if Console was never open.
//
// ----- Where the count is taken, and why it is honest -----
//
// The recorder is driven from `AdBannerQueue.recordBannerShown(for:)`,
// which is the ON-SCREEN presentation boundary, not the emission
// boundary. That choice matters:
//
//   * The orchestrator's `emitBannerItem` / `emitSuggestBanner` fire
//     per emission. Items are then de-duplicated, coalesced, dropped
//     for an episode/lifecycle mismatch, or discarded on host
//     teardown by `AdBannerQueue` — so an emission count would
//     OVERSTATE what the listener actually saw.
//   * `recordBannerShown(for:)` is guarded by
//     `didRecordShownForCurrentPresentation`, so repeated SwiftUI
//     exposure callbacks and in-place coalescing updates of the
//     currently visible card each count ONCE. It is the same boundary
//     the durable `banners_shown` aggregate (playhead-jw63.1) already
//     uses, so the two can never disagree.
//
// Re-presentation semantics, stated precisely: a card that is
// dismissed and later presented again as a genuinely new presentation
// — the orchestrator retired the old revision and emitted a fresh one,
// or the same window re-entered the lane after the lane advanced —
// counts AGAIN. That is deliberate: it is a second card the listener
// had to look at, which is exactly what "respect your attention" is
// measuring. What does NOT count twice is the same visible
// presentation being re-notified (exposure churn, coalescing update,
// assistive-control pause/resume).
//
// ----- Session scoping -----
//
// A row is one PLAYBACK SESSION of one episode, not an all-time
// per-episode accumulator. The scope key is the
// (episodeId, playbackLifecycleGeneration) pair the orchestrator
// stamps onto every item at `beginEpisode`; when either changes, the
// running index restarts at 1 and a new row opens. Replaying the same
// episode therefore does not inflate the previous listen's number.
// Rows never merge across app launches either, because the active
// session key is in-memory only.
//
// ----- Privacy -----
//
// The os_log breadcrumb is LOCAL and names the episode plainly, using
// the same `.public` annotation every identifier in `SkipOrchestrator`
// uses. The persisted row also holds the RAW episode id, because it
// never leaves the device in that form: `DiagnosticsBundleBuilder`
// projects it through `EpisodeIdHasher` (legal checklist item a) on
// the way into the bundle, exactly as the work-journal tail does. No
// title, feed URL, advertiser, product, or transcript text is stored
// or logged here at all.

import Foundation
import OSLog

// MARK: - Persisted row

/// One playback session's worth of banner presentations for one episode.
///
/// `episodeId` is the RAW identifier and is local-only. Every egress
/// path must hash it (`EpisodeIdHasher.hash(installID:episodeId:)`);
/// `DefaultBundle.BannerTallySummary` is the only projection that
/// exists, and it carries the hash.
struct BannerTallySession: Codable, Equatable, Sendable {

    /// Local-only, per-run session identity. Never projected into the
    /// diagnostics bundle — it exists so a row can be found and
    /// updated in place without depending on the episode id being
    /// unique across replays.
    let sessionKey: String

    /// RAW episode identifier. Local-only; hashed on egress.
    let episodeId: String

    /// Cards actually presented on screen in this session.
    let bannerCount: Int

    /// Of `bannerCount`, how many were the auto-skip tier.
    let autoSkippedCount: Int

    /// Of `bannerCount`, how many were the suggest tier — the tier
    /// `markOnly` spans route to, and therefore the number
    /// playhead-2350's hypothesis is about.
    let suggestCount: Int

    let firstShownAt: Date
    let lastShownAt: Date
}

// MARK: - Store

/// Main-actor-isolated, UserDefaults-backed per-episode banner tally.
///
/// Isolation and storage deliberately mirror `BannerFeedbackCounterStore`
/// (playhead-jw63.1): same actor, same defaults-backed JSON blob, no
/// event stream and no network behavior. This one is keyed per episode
/// session rather than being a single aggregate, which is the whole
/// reason it exists.
@MainActor
final class BannerTallyStore {

    /// Process-wide handle used by the production banner queue and read
    /// by the diagnostics hatches. Tests MUST construct their own
    /// instance against an isolated `UserDefaults` suite.
    static let shared = BannerTallyStore()

    /// Sessions retained. Forty listening sessions is comfortably more
    /// than a dogfood audit needs and still only a few kilobytes.
    static let maxSessions = 40

    private let defaults: UserDefaults
    private let storageKey: String
    private let now: @MainActor () -> Date
    private let logger: Logger

    /// The (episode, playback lifecycle) pair currently being counted.
    /// In-memory only: a relaunch always starts a fresh session even
    /// for the same episode, which is what keeps a resumed listen from
    /// being folded into the previous one's total.
    private var activeScope: Scope?
    private var activeSessionKey: String?

    private struct Scope: Equatable {
        let episodeId: String?
        let playbackLifecycleGeneration: UInt64?
    }

    init(
        defaults: UserDefaults = .standard,
        storageKey: String = "playhead.bannerTally.sessions.v1",
        logger: Logger = Logger(subsystem: "com.playhead", category: "BannerTally"),
        now: @escaping @MainActor () -> Date = { Date() }
    ) {
        self.defaults = defaults
        self.storageKey = storageKey
        self.logger = logger
        self.now = now
    }

    // MARK: - Read

    /// Every retained session row, oldest first. This is what the
    /// diagnostics hatches hand to the bundle builder.
    var sessions: [BannerTallySession] {
        guard let data = defaults.data(forKey: storageKey),
              let decoded = try? JSONDecoder().decode(
                  [BannerTallySession].self,
                  from: data
              )
        else {
            return []
        }
        return decoded
    }

    /// The running index within the session currently being counted, or
    /// 0 before its first card. Exposed for tests and for callers that
    /// want to assert the reset boundary without emitting a card.
    var currentSessionCount: Int {
        guard let activeSessionKey else { return 0 }
        return sessions.first { $0.sessionKey == activeSessionKey }?.bannerCount ?? 0
    }

    // MARK: - Write

    /// Record one on-screen banner presentation and return its 1-based
    /// index within the current episode session.
    ///
    /// Callers must invoke this exactly once per visible presentation;
    /// `AdBannerQueue.recordBannerShown(for:)` owns that guarantee.
    ///
    /// An item with no `episodeId` (isolated previews, unscoped test
    /// fixtures) still gets a live breadcrumb but is NOT persisted —
    /// a row whose episode reference would hash a placeholder is worse
    /// than no row, because it would silently pool unrelated cards.
    @discardableResult
    func recordPresentation(of item: AdSkipBannerItem) -> Int {
        let scope = Scope(
            episodeId: item.episodeId,
            playbackLifecycleGeneration: item.playbackLifecycleGeneration
        )
        if scope != activeScope {
            activeScope = scope
            activeSessionKey = UUID().uuidString
        }

        let index: Int
        if let episodeId = item.episodeId, !episodeId.isEmpty,
           let sessionKey = activeSessionKey {
            index = appendToSession(
                sessionKey: sessionKey,
                episodeId: episodeId,
                tier: item.tier
            )
        } else {
            index = 0
        }

        emitBreadcrumb(for: item, index: index)
        return index
    }

    /// Drop every retained row. Available to tests and to any future
    /// "clear diagnostics" affordance.
    func removeAll() {
        activeScope = nil
        activeSessionKey = nil
        defaults.removeObject(forKey: storageKey)
    }

    // MARK: - Private

    /// Upsert the active session row and return its new banner count.
    private func appendToSession(
        sessionKey: String,
        episodeId: String,
        tier: AdBannerTier
    ) -> Int {
        var rows = sessions
        let timestamp = now()
        let updated: BannerTallySession

        if let position = rows.firstIndex(where: { $0.sessionKey == sessionKey }) {
            let existing = rows[position]
            updated = BannerTallySession(
                sessionKey: existing.sessionKey,
                episodeId: existing.episodeId,
                bannerCount: Self.incremented(existing.bannerCount),
                autoSkippedCount: tier == .autoSkipped
                    ? Self.incremented(existing.autoSkippedCount)
                    : existing.autoSkippedCount,
                suggestCount: tier == .suggest
                    ? Self.incremented(existing.suggestCount)
                    : existing.suggestCount,
                firstShownAt: existing.firstShownAt,
                lastShownAt: timestamp
            )
            rows[position] = updated
        } else {
            updated = BannerTallySession(
                sessionKey: sessionKey,
                episodeId: episodeId,
                bannerCount: 1,
                autoSkippedCount: tier == .autoSkipped ? 1 : 0,
                suggestCount: tier == .suggest ? 1 : 0,
                firstShownAt: timestamp,
                lastShownAt: timestamp
            )
            rows.append(updated)
            // Cap from the OLD end: the session being counted right now
            // is the one the audit is about, so it must never be the
            // row that gets evicted.
            if rows.count > Self.maxSessions {
                rows.removeFirst(rows.count - Self.maxSessions)
            }
        }

        persist(rows)
        return updated.bannerCount
    }

    private func persist(_ rows: [BannerTallySession]) {
        guard let data = try? JSONEncoder().encode(rows) else { return }
        defaults.set(data, forKey: storageKey)
    }

    /// One `.notice` line per card.
    ///
    /// `.notice` rather than `.info` on purpose: this breadcrumb is the
    /// live surface, and `.info` is hidden in Console.app until the
    /// reader remembers to enable "Include Info Messages". One line per
    /// presented card is a handful per episode, which does not threaten
    /// the level's budget.
    ///
    /// Identifiers are marked `.public` to match every `os_log` call
    /// site in `SkipOrchestrator`; numeric interpolations are public by
    /// default. This is a LOCAL surface — nothing here is an egress
    /// path, so the episode id is named plainly.
    private func emitBreadcrumb(for item: AdSkipBannerItem, index: Int) {
        let start = Self.finiteForLog(item.adStartTime)
        let end = Self.finiteForLog(item.adEndTime)
        logger.notice(
            """
            banner-card index=\(index, privacy: .public) \
            tier=\(item.tier.rawValue, privacy: .public) \
            span=[\(start, format: .fixed(precision: 1))s, \
            \(end, format: .fixed(precision: 1))s] \
            episode=\(item.episodeId ?? "none", privacy: .public) \
            window=\(item.windowId, privacy: .public)
            """
        )
    }

    /// `%f` of a NaN/infinite span would print `nan`/`inf` into the one
    /// line the audit reads; clamp to 0 so a malformed span is obvious
    /// as a zero-width span rather than as log noise.
    private static func finiteForLog(_ value: Double) -> Double {
        value.isFinite ? value : 0
    }

    private static func incremented(_ value: Int) -> Int {
        value == Int.max ? Int.max : value + 1
    }
}
