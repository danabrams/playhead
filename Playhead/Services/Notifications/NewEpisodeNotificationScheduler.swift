// NewEpisodeNotificationScheduler.swift
// playhead-snp — Pure-orchestration service that turns a list of newly-
// discovered podcast episodes into local user notifications.
//
// Design summary:
//   - The scheduler takes a `[NewEpisodeCandidate]` (Sendable value
//     snapshots) and routes each one through a chain of gates:
//       1. App-wide toggle (UserPreferences.newEpisodeNotificationsEnabled)
//       2. Per-feed toggle (Podcast.notificationsEnabled, mirrored into
//          the candidate)
//       3. Already-played skip (defensive — fresh items shouldn't be)
//       4. "New" definition: published within `staleHorizon` of `now()`
//          (default 7 days). nil publishedAt is treated as fresh.
//       5. Dedup ledger — a process-pinned bounded set keyed by
//          `canonicalEpisodeKey`.
//   - Authorization: ask once when status is `.notDetermined`. Denied
//     status is silently respected (no scheduling, no re-ask). Authorized
//     status proceeds without a re-ask.
//   - Rate limiting: at most 10 individual notifications per call. If
//     the surviving set is > 10, fire the first 10 individually and a
//     single "X more new episodes available" summary at the tail.
//   - Tap payload: `userInfo["episodeKey"]`, `userInfo["feedURL"]`,
//     `userInfo["trigger"]` so a future delegate can route the tap.
//
// The scheduler is `@MainActor`-isolated so it can hand
// `UNNotificationRequest` (non-Sendable) into the injected scheduler
// without an isolation hop, and matches the existing
// `BatchNotificationService` pattern.

import Foundation
import OSLog
import UserNotifications

// MARK: - Public payload type

/// Sendable snapshot the scheduler accepts as input. Constructed at the
/// call site (BackgroundFeedRefreshService hook) from a SwiftData
/// `Podcast` + `Episode` pair so the scheduler itself never touches
/// SwiftData.
struct NewEpisodeCandidate: Sendable, Equatable {
    let feedURL: URL
    let feedTitle: String
    let canonicalEpisodeKey: String
    let episodeTitle: String
    let publishedAt: Date?
    let isPlayed: Bool
    let feedNotificationsEnabled: Bool
}

// MARK: - Scheduler

/// Orchestrates new-episode local notifications. One instance per
/// runtime; called from the feed-refresh hook once per refresh fire.
@MainActor
final class NewEpisodeNotificationScheduler {

    // MARK: - Collaborator protocols

    /// Wraps the `UNUserNotificationCenter.add` / `removePendingNotificationRequests`
    /// surface so tests can substitute a recording scheduler.
    protocol Scheduling: Sendable {
        @MainActor
        func add(_ request: UNNotificationRequest) async throws

        @MainActor
        func removePending(withIdentifiers identifiers: [String]) async
    }

    /// Wraps the authorization surface so tests can drive each branch
    /// (`.notDetermined`, `.denied`, `.authorized`) without touching the
    /// system center.
    protocol AuthorizationProviding: Sendable {
        @MainActor
        func authorizationStatus() async -> UNAuthorizationStatus

        @MainActor
        func requestAuthorization() async -> Bool
    }

    /// Process-pinned bounded dedup set. Production uses a UserDefaults-
    /// backed implementation (see `UserDefaultsNewEpisodeLedger`); tests
    /// substitute an in-memory implementation.
    protocol DedupLedger: Sendable {
        @MainActor
        func contains(_ key: String) -> Bool

        @MainActor
        func record(_ key: String)
    }

    // MARK: - Dependencies

    private let scheduler: any Scheduling
    private let authorizer: any AuthorizationProviding
    private let ledger: any DedupLedger
    private let appWideEnabledProvider: @MainActor () -> Bool
    private let now: @MainActor () -> Date
    private let staleHorizon: TimeInterval
    private let individualCap: Int

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "NewEpisodeNotifications"
    )

    // MARK: - Init

    init(
        scheduler: any Scheduling,
        authorizer: any AuthorizationProviding,
        ledger: any DedupLedger,
        appWideEnabledProvider: @escaping @MainActor () -> Bool = { true },
        now: @escaping @MainActor () -> Date = { Date() },
        staleHorizon: TimeInterval = 7 * 24 * 3600,
        individualCap: Int = 10
    ) {
        self.scheduler = scheduler
        self.authorizer = authorizer
        self.ledger = ledger
        self.appWideEnabledProvider = appWideEnabledProvider
        self.now = now
        self.staleHorizon = staleHorizon
        self.individualCap = individualCap
    }

    // MARK: - Public API

    /// Turn a list of candidates into local notifications. Idempotent
    /// across calls — each `canonicalEpisodeKey` will only ever produce
    /// one notification per process lifetime (subject to the ledger's
    /// persistence guarantees).
    func announce(_ candidates: [NewEpisodeCandidate]) async {
        // 0. App-wide off short-circuits everything. Do not consult
        //    authorization — we don't even want to ask.
        guard appWideEnabledProvider() else { return }

        // 1. Authorization gate.
        let status = await authorizer.authorizationStatus()
        switch status {
        case .denied, .ephemeral:
            // Silently respect; never re-ask. `.ephemeral` is App-Clip-
            // adjacent and should not produce user-visible local notifs.
            return
        case .authorized, .provisional:
            break
        case .notDetermined:
            let granted = await authorizer.requestAuthorization()
            if !granted { return }
        @unknown default:
            return
        }

        // 2. Per-candidate gating: per-feed toggle, isPlayed, stale
        //    horizon, dedup ledger.
        let nowDate = now()
        let horizonStart = nowDate.addingTimeInterval(-staleHorizon)

        let surviving = candidates.compactMap { candidate -> NewEpisodeCandidate? in
            guard candidate.feedNotificationsEnabled else { return nil }
            guard !candidate.isPlayed else { return nil }
            if let published = candidate.publishedAt, published < horizonStart {
                return nil
            }
            if ledger.contains(candidate.canonicalEpisodeKey) { return nil }
            return candidate
        }

        guard !surviving.isEmpty else { return }

        // 3. Rate limit: at most `individualCap` individual notifications;
        //    if there are more surviving candidates, fire a single
        //    summary notification at the tail.
        let individualSlice = surviving.prefix(individualCap)
        let overflow = surviving.count - individualSlice.count

        for candidate in individualSlice {
            await fireIndividual(for: candidate)
        }

        if overflow > 0 {
            await fireSummary(overflowCount: overflow)
        }
    }

    /// Cancel previously-scheduled-but-not-yet-delivered notifications.
    /// Used when the user toggles app-wide notifications off, so any
    /// queued local notifications don't fire after the switch.
    func cancelPendingNotifications(identifiers: [String]) async {
        await scheduler.removePending(withIdentifiers: identifiers)
    }

    // MARK: - Private firing

    private func fireIndividual(for candidate: NewEpisodeCandidate) async {
        let content = UNMutableNotificationContent()
        content.title = candidate.feedTitle
        content.body = candidate.episodeTitle
        content.sound = .default
        content.categoryIdentifier = Self.categoryIdentifier
        content.userInfo = [
            "episodeKey": candidate.canonicalEpisodeKey,
            "feedURL": candidate.feedURL.absoluteString,
            "trigger": "newEpisode",
        ]

        let request = UNNotificationRequest(
            identifier: Self.requestIdentifier(for: candidate.canonicalEpisodeKey),
            content: content,
            trigger: nil // immediate
        )

        do {
            try await scheduler.add(request)
            ledger.record(candidate.canonicalEpisodeKey)
            logger.info(
                "Scheduled new-episode notification for \(candidate.canonicalEpisodeKey, privacy: .public)"
            )
        } catch {
            logger.error(
                "Failed to schedule new-episode notification: \(String(describing: error), privacy: .public)"
            )
        }
    }

    private func fireSummary(overflowCount: Int) async {
        let content = UNMutableNotificationContent()
        content.title = "New Episodes"
        content.body = "\(overflowCount) more new episodes available"
        content.sound = .default
        content.categoryIdentifier = Self.summaryCategoryIdentifier
        content.userInfo = [
            "trigger": "newEpisodeSummary",
            "overflow": overflowCount,
        ]

        let request = UNNotificationRequest(
            identifier: "new-episode-summary-\(UUID().uuidString)",
            content: content,
            trigger: nil
        )

        do {
            try await scheduler.add(request)
        } catch {
            logger.error(
                "Failed to schedule new-episode summary: \(String(describing: error), privacy: .public)"
            )
        }
    }

    // MARK: - Identifiers

    static let categoryIdentifier: String = "NEW_EPISODE"
    static let summaryCategoryIdentifier: String = "NEW_EPISODE_SUMMARY"

    /// Request identifier for an individual new-episode notification.
    /// Stable across retries so a re-fire of the same episode key would
    /// idempotently replace any pending duplicate (the ledger should
    /// have already prevented the second call from reaching here, but
    /// the stable identifier is the second line of defense).
    static func requestIdentifier(for episodeKey: String) -> String {
        "new-episode-\(episodeKey)"
    }
}

// MARK: - Production Scheduling adapter

/// Production scheduler backed by `UNUserNotificationCenter.current()`.
/// Mirrors `SystemNotificationScheduler` in `BatchNotificationService`.
struct SystemNewEpisodeNotificationScheduler: NewEpisodeNotificationScheduler.Scheduling {
    func add(_ request: UNNotificationRequest) async throws {
        try await UNUserNotificationCenter.current().add(request)
    }

    func removePending(withIdentifiers identifiers: [String]) async {
        UNUserNotificationCenter.current()
            .removePendingNotificationRequests(withIdentifiers: identifiers)
    }
}

// MARK: - Production AuthorizationProviding adapter

struct SystemNewEpisodeAuthorizationProvider: NewEpisodeNotificationScheduler.AuthorizationProviding {
    func authorizationStatus() async -> UNAuthorizationStatus {
        await UNUserNotificationCenter.current().notificationSettings().authorizationStatus
    }

    func requestAuthorization() async -> Bool {
        do {
            return try await UNUserNotificationCenter.current()
                .requestAuthorization(options: [.alert, .sound, .badge])
        } catch {
            return false
        }
    }
}
