// NewEpisodeNotificationSchedulerTests.swift
// playhead-snp — TDD-driven contract for the new-episode notification
// scheduler. The scheduler is pure orchestration backed by an injected
// notification-center protocol and a dedup ledger.
//
// Test ladder:
//   1. Schedules one notification per new episode
//   2. Per-show notificationsEnabled == false skips that feed
//   3. App-wide notificationsEnabled == false skips everything
//   4. Already-announced episode (in ledger) does not re-fire
//   5. isPlayed episodes are skipped
//   6. Items older than 7 days are skipped
//   7. > 10 episodes => 10 individual + 1 summary
//   8. userInfo carries episodeKey + feedURL for tap routing
//   9. Permission ask happens once; persisted decision is honored
//  10. Authorization-denied: no requests added

import Foundation
import Testing
import UserNotifications

@testable import Playhead

// MARK: - Recording scheduler / authorizer

@MainActor
private final class RecordingScheduler: NewEpisodeNotificationScheduler.Scheduling {
    private(set) var requests: [UNNotificationRequest] = []

    func add(_ request: UNNotificationRequest) async throws {
        requests.append(request)
    }

    func removePending(withIdentifiers identifiers: [String]) async {
        requests.removeAll { identifiers.contains($0.identifier) }
    }

    func snapshot() -> [UNNotificationRequest] { requests }
}

@MainActor
private final class StubAuthorizer: NewEpisodeNotificationScheduler.AuthorizationProviding {
    var statusValue: UNAuthorizationStatus = .authorized
    var requestCallCount = 0
    var grantOnRequest = true

    func authorizationStatus() async -> UNAuthorizationStatus { statusValue }

    func requestAuthorization() async -> Bool {
        requestCallCount += 1
        return grantOnRequest
    }
}

@MainActor
private final class InMemoryLedger: NewEpisodeNotificationScheduler.DedupLedger {
    var seen: Set<String> = []
    func contains(_ key: String) -> Bool { seen.contains(key) }
    func record(_ key: String) { seen.insert(key) }
    func reset() { seen.removeAll() }
    func count() -> Int { seen.count }
}

// MARK: - Suite

@Suite("NewEpisodeNotificationScheduler — pure orchestration (playhead-snp)")
@MainActor
struct NewEpisodeNotificationSchedulerTests {

    private static func makeService(
        scheduler: RecordingScheduler,
        authorizer: StubAuthorizer,
        ledger: InMemoryLedger,
        appWideEnabled: Bool = true,
        now: Date = Date(timeIntervalSince1970: 1_700_000_000)
    ) -> NewEpisodeNotificationScheduler {
        NewEpisodeNotificationScheduler(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger,
            appWideEnabledProvider: { appWideEnabled },
            now: { now }
        )
    }

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    private static func candidate(
        feedURL: String = "https://feed.example.com/a.xml",
        feedTitle: String = "The Show",
        episodeKey: String = "https://feed.example.com/a.xml::ep-1",
        episodeTitle: String = "An Episode",
        publishedAt: Date? = t0,
        isPlayed: Bool = false,
        feedNotificationsEnabled: Bool = true
    ) -> NewEpisodeCandidate {
        NewEpisodeCandidate(
            feedURL: URL(string: feedURL)!,
            feedTitle: feedTitle,
            canonicalEpisodeKey: episodeKey,
            episodeTitle: episodeTitle,
            publishedAt: publishedAt,
            isPlayed: isPlayed,
            feedNotificationsEnabled: feedNotificationsEnabled
        )
    }

    // MARK: - 1. Schedules a notification per new episode

    @Test("Each new episode produces one notification request")
    func schedulesOnePerEpisode() async throws {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([
            Self.candidate(episodeKey: "k1", episodeTitle: "Ep One"),
            Self.candidate(episodeKey: "k2", episodeTitle: "Ep Two"),
        ])

        #expect(scheduler.snapshot().count == 2)
        #expect(scheduler.snapshot()[0].content.title == "The Show")
        #expect(scheduler.snapshot()[0].content.body == "Ep One")
        #expect(scheduler.snapshot()[1].content.body == "Ep Two")
    }

    // MARK: - 2. Per-show toggle

    @Test("Per-show notificationsEnabled == false skips the show")
    func perShowToggleRespected() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([
            Self.candidate(episodeKey: "k-on", feedNotificationsEnabled: true),
            Self.candidate(episodeKey: "k-off", feedNotificationsEnabled: false),
        ])

        #expect(scheduler.snapshot().count == 1)
        #expect(scheduler.snapshot()[0].content.userInfo["episodeKey"] as? String == "k-on")
    }

    // MARK: - 3. App-wide toggle

    @Test("App-wide off => zero requests")
    func appWideToggleRespected() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger,
            appWideEnabled: false
        )

        await service.announce([
            Self.candidate(episodeKey: "k1"),
            Self.candidate(episodeKey: "k2"),
        ])

        #expect(scheduler.snapshot().isEmpty)
    }

    // MARK: - 4. Dedup ledger

    @Test("Already-announced episode does not re-fire")
    func dedupLedgerSuppresses() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        ledger.record("k-already")

        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([
            Self.candidate(episodeKey: "k-already"),
            Self.candidate(episodeKey: "k-fresh"),
        ])

        #expect(scheduler.snapshot().count == 1)
        #expect(scheduler.snapshot()[0].content.userInfo["episodeKey"] as? String == "k-fresh")
    }

    @Test("Successful announce records the episode in the ledger")
    func dedupLedgerWritesOnAnnounce() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([Self.candidate(episodeKey: "k1")])

        #expect(ledger.contains("k1"))
    }

    // MARK: - 5. isPlayed skip

    @Test("Episodes that are already played are skipped")
    func skipsAlreadyPlayed() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([
            Self.candidate(episodeKey: "k1", isPlayed: true),
            Self.candidate(episodeKey: "k2", isPlayed: false),
        ])

        #expect(scheduler.snapshot().count == 1)
        #expect(scheduler.snapshot()[0].content.userInfo["episodeKey"] as? String == "k2")
    }

    // MARK: - 6. Old items skip

    @Test("Episodes published more than 7 days before now are skipped")
    func skipsOldEpisodes() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger,
            now: now
        )

        let stale = now.addingTimeInterval(-8 * 24 * 3600)
        let fresh = now.addingTimeInterval(-1 * 24 * 3600)

        await service.announce([
            Self.candidate(episodeKey: "k-stale", publishedAt: stale),
            Self.candidate(episodeKey: "k-fresh", publishedAt: fresh),
        ])

        #expect(scheduler.snapshot().count == 1)
        #expect(scheduler.snapshot()[0].content.userInfo["episodeKey"] as? String == "k-fresh")
    }

    @Test("Episodes with nil publishedAt are scheduled (we treat unknown date as fresh)")
    func nilPublishedAtScheduled() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([Self.candidate(episodeKey: "k1", publishedAt: nil)])

        #expect(scheduler.snapshot().count == 1)
    }

    // MARK: - 7. Rate limiting

    @Test("More than 10 candidates => 10 individual + 1 summary")
    func rateLimitsAtTenPlusSummary() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        let candidates = (0..<13).map { i in
            Self.candidate(
                episodeKey: "k-\(i)",
                episodeTitle: "Ep \(i)"
            )
        }
        await service.announce(candidates)

        let requests = scheduler.snapshot()
        #expect(requests.count == 11)
        // The summary comes last and is identified by trigger == "summary".
        #expect(requests.last?.content.userInfo["trigger"] as? String == "newEpisodeSummary")
        #expect(requests.last?.content.body.contains("3") == true)
    }

    @Test("Exactly 10 candidates => 10 individual, no summary")
    func tenCandidatesNoSummary() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        let candidates = (0..<10).map { i in
            Self.candidate(episodeKey: "k-\(i)")
        }
        await service.announce(candidates)

        #expect(scheduler.snapshot().count == 10)
        #expect(
            scheduler.snapshot().contains(where: {
                $0.content.userInfo["trigger"] as? String == "newEpisodeSummary"
            }) == false
        )
    }

    // MARK: - 8. userInfo for tap routing

    @Test("userInfo carries episodeKey and feedURL")
    func userInfoCarriesRoutingPayload() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([
            Self.candidate(
                feedURL: "https://feed.example.com/show.xml",
                episodeKey: "the-key"
            )
        ])

        let userInfo = scheduler.snapshot()[0].content.userInfo
        #expect(userInfo["episodeKey"] as? String == "the-key")
        #expect(userInfo["feedURL"] as? String == "https://feed.example.com/show.xml")
        #expect(userInfo["trigger"] as? String == "newEpisode")
    }

    // MARK: - 9 & 10. Authorization

    @Test("Permission is requested when status is .notDetermined")
    func requestsPermissionWhenNotDetermined() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        authorizer.statusValue = .notDetermined
        authorizer.grantOnRequest = true
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([Self.candidate(episodeKey: "k1")])

        #expect(authorizer.requestCallCount == 1)
        #expect(scheduler.snapshot().count == 1)
    }

    @Test("Denied authorization => no requests added, no further asks")
    func deniedAuthorizationSilent() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        authorizer.statusValue = .denied
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([Self.candidate(episodeKey: "k1")])

        #expect(scheduler.snapshot().isEmpty)
        #expect(authorizer.requestCallCount == 0)
    }

    @Test("Authorized status => no permission request issued")
    func authorizedDoesNotReask() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        authorizer.statusValue = .authorized
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([Self.candidate(episodeKey: "k1")])

        #expect(authorizer.requestCallCount == 0)
        #expect(scheduler.snapshot().count == 1)
    }

    @Test("Permission request denial: no requests scheduled, ledger untouched")
    func notDeterminedThenDenied() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        authorizer.statusValue = .notDetermined
        authorizer.grantOnRequest = false
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([Self.candidate(episodeKey: "k1")])

        #expect(scheduler.snapshot().isEmpty)
        #expect(ledger.contains("k1") == false)
    }

    // MARK: - cancelAllPending

    @Test("cancelAllPending removes outstanding pending requests via the scheduler")
    func cancelAllPendingForwardsToScheduler() async {
        let scheduler = RecordingScheduler()
        let authorizer = StubAuthorizer()
        let ledger = InMemoryLedger()
        let service = Self.makeService(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        await service.announce([
            Self.candidate(episodeKey: "k1"),
            Self.candidate(episodeKey: "k2"),
        ])
        #expect(scheduler.snapshot().count == 2)

        let identifiers = scheduler.snapshot().map(\.identifier)
        await service.cancelPendingNotifications(identifiers: identifiers)

        #expect(scheduler.snapshot().isEmpty)
    }
}
