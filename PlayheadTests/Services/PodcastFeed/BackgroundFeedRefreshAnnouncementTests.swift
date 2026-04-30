// BackgroundFeedRefreshAnnouncementTests.swift
// playhead-snp — tests for the post-refresh "announcer" hop. After the
// refresh loop has discovered new episodes, the service hands them to
// an injected `NewEpisodeAnnouncing` collaborator. The contract is:
//   - Announce is called once per refresh fire, with the union of all
//     discovered new episodes across feeds (whether or not they were
//     enqueued for download).
//   - Announce is NOT called when no new episodes are discovered.
//   - Announce respects the same expiration boundary as the rest of
//     the loop — if the BG task has expired, we skip the announcer to
//     stay inside the post-expiration grace window.

import BackgroundTasks
import Foundation
import Testing
@testable import Playhead

// MARK: - Test doubles

private actor RecordingAnnouncer: NewEpisodeAnnouncing {
    private(set) var calls: [[FeedRefreshNewEpisode]] = []

    func announce(newEpisodes: [FeedRefreshNewEpisode]) async {
        calls.append(newEpisodes)
    }
}

private actor MinimalEnumerator: PodcastEnumerating {
    let snapshots: [FeedRefreshPodcastSnapshot]
    init(_ snapshots: [FeedRefreshPodcastSnapshot]) {
        self.snapshots = snapshots
    }
    func enumeratePodcasts() async -> [FeedRefreshPodcastSnapshot] { snapshots }
}

private actor MinimalRefresher: FeedRefreshing {
    var newEpisodes: [URL: [FeedRefreshNewEpisode]] = [:]

    func setNew(_ list: [FeedRefreshNewEpisode], for feedURL: URL) {
        newEpisodes[feedURL] = list
    }

    func refreshEpisodes(
        feedURL: URL,
        existingEpisodeGUIDs _: Set<String>
    ) async throws -> [FeedRefreshNewEpisode] {
        newEpisodes[feedURL] ?? []
    }
}

private struct NoopDownloader: AutoDownloadEnqueueing {
    func enqueueBackgroundDownload(episodeId _: String, from _: URL) async {}
}

private struct OffSettings: DownloadsSettingsProviding {
    func currentAutoDownloadSetting() -> AutoDownloadOnSubscribe { .off }
}

// MARK: - Suite

@Suite("BackgroundFeedRefreshService — announcer hop (playhead-snp)")
struct BackgroundFeedRefreshAnnouncementTests {

    @Test("Announcer receives every newly-discovered episode after the refresh loop")
    func announcerReceivesAllNew() async throws {
        let feedA = URL(string: "https://example.com/a.xml")!
        let feedB = URL(string: "https://example.com/b.xml")!
        let enumerator = MinimalEnumerator([
            FeedRefreshPodcastSnapshot(feedURL: feedA, existingEpisodeGUIDs: []),
            FeedRefreshPodcastSnapshot(feedURL: feedB, existingEpisodeGUIDs: []),
        ])
        let refresher = MinimalRefresher()
        await refresher.setNew(
            [
                FeedRefreshNewEpisode(
                    canonicalEpisodeKey: "a-1",
                    audioURL: URL(string: "https://example.com/a-1.mp3")!,
                    publishedAt: .now
                )
            ],
            for: feedA
        )
        await refresher.setNew(
            [
                FeedRefreshNewEpisode(
                    canonicalEpisodeKey: "b-1",
                    audioURL: URL(string: "https://example.com/b-1.mp3")!,
                    publishedAt: .now
                ),
                FeedRefreshNewEpisode(
                    canonicalEpisodeKey: "b-2",
                    audioURL: URL(string: "https://example.com/b-2.mp3")!,
                    publishedAt: .now
                ),
            ],
            for: feedB
        )

        let announcer = RecordingAnnouncer()
        let service = BackgroundFeedRefreshService(
            enumerator: enumerator,
            refresher: refresher,
            downloader: NoopDownloader(),
            settingsProvider: OffSettings(),
            taskScheduler: StubTaskScheduler(),
            newEpisodeAnnouncer: announcer
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let calls = await announcer.calls
        #expect(calls.count == 1, "Announcer should be invoked once per fire when work was found")
        let keys = Set(calls.first?.map(\.canonicalEpisodeKey) ?? [])
        #expect(keys == ["a-1", "b-1", "b-2"])
    }

    @Test("Announcer is not called when no new episodes are discovered")
    func announcerSkippedWhenNothingNew() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let enumerator = MinimalEnumerator([
            FeedRefreshPodcastSnapshot(feedURL: feed, existingEpisodeGUIDs: ["existing"])
        ])
        let refresher = MinimalRefresher()
        // refresher returns []
        let announcer = RecordingAnnouncer()
        let service = BackgroundFeedRefreshService(
            enumerator: enumerator,
            refresher: refresher,
            downloader: NoopDownloader(),
            settingsProvider: OffSettings(),
            taskScheduler: StubTaskScheduler(),
            newEpisodeAnnouncer: announcer
        )

        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)

        let calls = await announcer.calls
        #expect(calls.isEmpty, "Announcer should not fire when refresh found nothing new")
    }

    @Test("Default service (no announcer wired) is still safe to construct + run")
    func defaultAnnouncerIsNoOp() async throws {
        let feed = URL(string: "https://example.com/a.xml")!
        let enumerator = MinimalEnumerator([
            FeedRefreshPodcastSnapshot(feedURL: feed, existingEpisodeGUIDs: [])
        ])
        let refresher = MinimalRefresher()
        await refresher.setNew(
            [FeedRefreshNewEpisode(
                canonicalEpisodeKey: "k",
                audioURL: URL(string: "https://example.com/k.mp3")!,
                publishedAt: .now
            )],
            for: feed
        )

        // Construct WITHOUT supplying newEpisodeAnnouncer — must still run.
        let service = BackgroundFeedRefreshService(
            enumerator: enumerator,
            refresher: refresher,
            downloader: NoopDownloader(),
            settingsProvider: OffSettings(),
            taskScheduler: StubTaskScheduler()
        )
        let task = StubBackgroundTask()
        await service.handleFeedRefreshTask(task)
        #expect(task.completedSuccess == true)
    }
}
