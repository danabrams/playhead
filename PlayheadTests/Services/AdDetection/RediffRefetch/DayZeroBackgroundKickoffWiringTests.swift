// DayZeroBackgroundKickoffWiringTests.swift
// playhead-cnql: day-zero rediff must start for a downloaded episode whatever
// started the download — and whatever kind of process the completion lands in.
//
// WHAT WAS BROKEN, AND WHY NOTHING CAUGHT IT. playhead-4dqe built the seam
// (`DownloadManager.setBackgroundDownloadCompletionObserver`) and installed it
// from `PlayheadApp.task` — a SwiftUI SCENE modifier. iOS relaunches this app
// with no scene to drain background URLSession events, and wakes it the same way
// for a BGTask; in those processes `.task` never runs, so no observer is ever
// installed and `notifyBackgroundDownloadCompleted` took a bare
// `guard let observer else { return }`. The kickoff vanished with no attempt, no
// give-up and no row in `rediff_day_zero_kickoffs` — indistinguishable from a
// download that never happened, which is why the 4dqe suite (all coordinator
// and policy level) reported healthy while the field pull of 2026-08-10 showed
// 16 of 27 post-4dqe assets with no kickoff row of any kind.
//
// 4dqe's tests never touched the `DownloadManager` seam at all: `coordinator*`
// starts at `requestKickoff`, i.e. one step AFTER the drop. These three suites
// cover the step that was missing.
//
//   1. `DayZeroBackgroundKickoffHandoffTests` — the hand-off is
//      ORDER-INDEPENDENT. A completion that lands before an observer exists is
//      delivered when one arrives, and a completion that lands after is
//      delivered exactly once and never replayed.
//   2. `DayZeroBackgroundKickoffRequestTests` — the request survives a process
//      with NO SwiftData resolver, which is the population this bead is for.
//   3. `DayZeroBackgroundKickoffWiringCanaryTests` — the observer is installed
//      from the LAUNCH path, not from a scene attach. Structural, because no
//      unit test can stand up a scene-less relaunch.
//
// Offline: a temp-dir `DownloadManager`, a pure function, and the repo's own
// source text. No network, no device, no scene.

import Foundation
import Testing
import XCTest

@testable import Playhead

// MARK: - 1. The DownloadManager hand-off

private actor KickoffHandoffJournal: WorkJournalRecording {
    func recordFinalized(episodeId: String) async {}
    func recordFailed(episodeId: String, cause: InternalMissCause) async {}
    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {}
    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {}
}

/// Collects what the day-0 observer was told. An actor because the observer is
/// `@Sendable` and fires off the DownloadManager actor.
private actor ObservedCompletions {
    private(set) var seen: [(episodeId: String, sourceURL: URL?)] = []

    func record(episodeId: String, sourceURL: URL?) {
        seen.append((episodeId: episodeId, sourceURL: sourceURL))
    }

    var episodeIds: [String] { seen.map(\.episodeId) }
    var count: Int { seen.count }
    var sourceURLs: [URL?] { seen.map(\.sourceURL) }
}

@Suite("Day-0 background-download hand-off is order-independent (playhead-cnql)")
struct DayZeroBackgroundKickoffHandoffTests {

    private func makeManager(in dir: URL) async throws -> DownloadManager {
        let manager = DownloadManager(
            cacheDirectory: dir,
            workJournalRecorder: KickoffHandoffJournal()
        )
        try await manager.bootstrap()
        return manager
    }

    /// Completes a background download the way the URLSession delegate does,
    /// leaving a servable pinned artifact.
    private func completeBackgroundDownload(
        manager: DownloadManager,
        dir: URL,
        episodeId: String,
        taskIdentifier: Int,
        sourceURL: URL
    ) async throws {
        let identity = BackgroundTransferIdentity(
            sessionIdentifier: "cnql-session",
            taskIdentifier: taskIdentifier
        )
        let staged = dir.appendingPathComponent("\(episodeId)-staged.mp3")
        try Data(repeating: 0x5A, count: 256).write(to: staged)
        await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId,
            identity: identity
        )
        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: staged,
            originalURL: sourceURL,
            metadata: nil,
            transferIdentity: identity
        )
    }

    @Test("""
    THE FIX: a completion that lands BEFORE any observer is installed is \
    delivered when one arrives — the background-relaunch case, where \
    `PlayheadApp.task` never runs
    """)
    func completionBeforeObserverIsDeliveredOnInstall() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = try await makeManager(in: dir)
        let episodeId = "cnql-completed-before-observer"
        let sourceURL = try #require(
            URL(string: "https://example.com/cnql-before.mp3")
        )

        try await completeBackgroundDownload(
            manager: manager,
            dir: dir,
            episodeId: episodeId,
            taskIdentifier: 71,
            sourceURL: sourceURL
        )
        #expect(
            await manager.cachedFileURL(for: episodeId) != nil,
            """
            ANTI-VACUITY: the fixture must actually land a servable artifact. \
            `handleBackgroundDownloadComplete` has seven early returns before \
            `notifyBackgroundDownloadCompleted`, and taking any of them would make \
            this test pass for the wrong reason.
            """
        )

        // ANTI-VACUITY: the completion must be HELD, not merely un-delivered.
        // Pre-fix this reads 0 — `notifyBackgroundDownloadCompleted` returned
        // on its nil-observer guard and the kickoff was gone. This assertion is
        // what distinguishes "buffered" from "there was nothing to deliver".
        #expect(await manager._pendingBackgroundDownloadCompletionCountForTesting() == 1)

        let observed = ObservedCompletions()
        await manager.setBackgroundDownloadCompletionObserver {
            @Sendable episode, url in
            Task { await observed.record(episodeId: episode, sourceURL: url) }
        }

        #expect(
            await pollUntil { await observed.episodeIds == [episodeId] },
            "installing the observer must drain the completion the pre-fix build dropped"
        )
        #expect(await observed.sourceURLs == [sourceURL])
        #expect(await manager._pendingBackgroundDownloadCompletionCountForTesting() == 0)
    }

    @Test("""
    ANTI-VACUITY CONTROL: a completion that lands AFTER the observer is \
    installed is delivered exactly once, and is NOT also buffered for replay
    """)
    func completionAfterObserverIsDeliveredOnceAndNotBuffered() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = try await makeManager(in: dir)
        let episodeId = "cnql-completed-after-observer"
        let sourceURL = try #require(
            URL(string: "https://example.com/cnql-after.mp3")
        )

        let first = ObservedCompletions()
        await manager.setBackgroundDownloadCompletionObserver {
            @Sendable episode, url in
            Task { await first.record(episodeId: episode, sourceURL: url) }
        }
        try await completeBackgroundDownload(
            manager: manager,
            dir: dir,
            episodeId: episodeId,
            taskIdentifier: 72,
            sourceURL: sourceURL
        )

        #expect(await pollUntil { await first.episodeIds == [episodeId] })
        #expect(await manager._pendingBackgroundDownloadCompletionCountForTesting() == 0)

        // A second install must find an EMPTY backlog. If the notify path
        // buffered as well as delivered, every later install would replay every
        // completion the process ever saw — a duplicate kickoff channel wearing
        // a fix's clothes.
        let second = ObservedCompletions()
        await manager.setBackgroundDownloadCompletionObserver {
            @Sendable episode, url in
            Task { await second.record(episodeId: episode, sourceURL: url) }
        }
        try? await Task.sleep(nanoseconds: 50_000_000)
        #expect(await second.count == 0)
        #expect(await first.count == 1)
    }

    @Test("""
    the buffer is BOUNDED and per-episode: a process that never installs an \
    observer cannot grow it without limit, and a re-delivered completion \
    replaces its predecessor rather than queueing a duplicate
    """)
    func bufferIsBoundedAndKeyedByEpisode() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = try await makeManager(in: dir)
        let cap = DownloadManager.maxPendingBackgroundDownloadCompletions

        for index in 0..<(cap + 8) {
            await manager._bufferBackgroundDownloadCompletionForTesting(
                episodeId: "cnql-overflow-\(index)",
                sourceURL: URL(string: "https://example.com/\(index).mp3")
            )
        }
        #expect(await manager._pendingBackgroundDownloadCompletionCountForTesting() == cap)

        // The per-episode key: re-buffering an episode already queued must not
        // add a second entry. ANTI-VACUITY for the count above — without the
        // dedup, `cap` would be reachable by repeating ONE episode, which would
        // make the cap assertion say nothing about distinct downloads.
        let before = await manager._pendingBackgroundDownloadCompletionCountForTesting()
        let repeated = "cnql-overflow-\(cap + 7)"
        await manager._bufferBackgroundDownloadCompletionForTesting(
            episodeId: repeated,
            sourceURL: URL(string: "https://example.com/repeat.mp3")
        )
        #expect(await manager._pendingBackgroundDownloadCompletionCountForTesting() == before)

        // …and the REPLACEMENT is what survives: the newest URL for that
        // episode, not the one it was first queued with.
        let observed = ObservedCompletions()
        await manager.setBackgroundDownloadCompletionObserver {
            @Sendable episode, url in
            Task { await observed.record(episodeId: episode, sourceURL: url) }
        }
        #expect(await pollUntil { await observed.count == cap })
        let delivered = await observed.seen.filter { $0.episodeId == repeated }
        #expect(delivered.count == 1)
        #expect(delivered.first?.sourceURL == URL(string: "https://example.com/repeat.mp3"))
    }
}

// MARK: - 2. The request the observer builds

@Suite("Day-0 background kickoff request (playhead-cnql)")
struct DayZeroBackgroundKickoffRequestTests {

    private let downloadURL = URL(string: "https://cdn.example.com/followed.mp3")!
    private let feedURL = URL(string: "https://feed.example.com/current.mp3")!

    @Test("""
    THE FIX: with NO SwiftData facts — the background-relaunch / BGTask-only \
    process — the kickoff still goes, on the URL the download itself recorded
    """)
    func noFactsFallsBackToTheDownloadURL() {
        let request = DayZeroBackgroundKickoff.request(
            episodeId: "ep-1",
            facts: nil,
            fallbackURL: downloadURL,
            enqueuedAt: 1_000
        )
        #expect(request?.enclosureURL == downloadURL)
        #expect(request?.publishedAt == nil)
        #expect(request?.source == .backgroundDownload)
        #expect(request?.enqueuedAt == 1_000)
        #expect(request?.episodeId == "ep-1")
    }

    @Test("""
    ANTI-VACUITY CONTROL: when the facts ARE available the FEED's current \
    enclosure wins over the URL the download followed, and the publish date is \
    carried so the drain can order newest-first
    """)
    func factsWinOverTheDownloadURL() {
        let request = DayZeroBackgroundKickoff.request(
            episodeId: "ep-2",
            facts: DayZeroKickoffEpisodeFacts(
                enclosureURL: feedURL,
                publishedAt: 12_345
            ),
            fallbackURL: downloadURL,
            enqueuedAt: 2_000
        )
        #expect(request?.enclosureURL == feedURL)
        #expect(request?.publishedAt == 12_345)
    }

    @Test("a row with no audio URL falls back too — a present row is not a present URL")
    func factsWithoutURLFallBack() {
        let request = DayZeroBackgroundKickoff.request(
            episodeId: "ep-3",
            facts: DayZeroKickoffEpisodeFacts(enclosureURL: nil, publishedAt: 999),
            fallbackURL: downloadURL,
            enqueuedAt: 3_000
        )
        #expect(request?.enclosureURL == downloadURL)
        // The date is still known even when the URL is not — the two are
        // independent, and dropping the date here would silently demote the
        // episode to the back of a contended drain.
        #expect(request?.publishedAt == 999)
    }

    @Test("no URL from EITHER source is the one honest nil — there is nothing to re-fetch")
    func noURLAnywhereYieldsNoRequest() {
        #expect(DayZeroBackgroundKickoff.request(
            episodeId: "ep-4",
            facts: DayZeroKickoffEpisodeFacts(enclosureURL: nil, publishedAt: nil),
            fallbackURL: nil,
            enqueuedAt: 4_000
        ) == nil)
        #expect(DayZeroBackgroundKickoff.request(
            episodeId: "ep-5",
            facts: nil,
            fallbackURL: nil,
            enqueuedAt: 5_000
        ) == nil)
    }

    @Test("""
    the source is ALWAYS `.backgroundDownload` — the ledger's whole value is \
    telling a support engineer which path is broken
    """)
    func sourceIsAlwaysBackgroundDownload() {
        for facts in [
            nil,
            DayZeroKickoffEpisodeFacts(enclosureURL: feedURL, publishedAt: nil),
            DayZeroKickoffEpisodeFacts(enclosureURL: nil, publishedAt: 1)
        ] as [DayZeroKickoffEpisodeFacts?] {
            let request = DayZeroBackgroundKickoff.request(
                episodeId: "ep-6",
                facts: facts,
                fallbackURL: downloadURL,
                enqueuedAt: 6_000
            )
            #expect(request?.source == .backgroundDownload)
        }
    }
}

// MARK: - 3. The install site

/// XCTest, matching `PlayheadRuntimeWiringSourceCanaryTests`: a source canary is
/// the only instrument that can see WHERE a wiring call lives, and no unit test
/// can stand up the scene-less relaunch this bead is about.
final class DayZeroBackgroundKickoffWiringCanaryTests: XCTestCase {

    /// The observer install must sit on the LAUNCH path in `PlayheadRuntime.init`
    /// — between the BGTask handler registration and the deferred bootstrap
    /// Task's `downloadManager.bootstrap()` — and must NOT be reachable only
    /// from a scene attach.
    ///
    /// Both halves matter. A regression that moves the call back into
    /// `attachRediffEnclosureResolver` (or any other
    /// `PlayheadApp.task`-invoked attach) would compile, pass every coordinator
    /// test, and silently restore the exact drop playhead-cnql closed: no
    /// observer in a headless relaunch, therefore no kickoff, therefore no row
    /// anywhere saying so.
    func testDayZeroBackgroundKickoffIsInstalledAtLaunchNotFromTheScene() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )

        // ANTI-VACUITY: every anchor must exist. A renamed anchor must fail
        // loudly rather than let the ordering assertions pass over nothing.
        guard let bgTaskRegistration = source.range(
            of: "BackgroundFeedRefreshService.registerTaskHandler()"
        ) else {
            XCTFail(
                "Could not locate `BackgroundFeedRefreshService.registerTaskHandler()` — " +
                "the launch-path anchor moved and this canary needs re-anchoring."
            )
            return
        }
        guard let bootstrap = source.range(
            of: "try await downloadManager.bootstrap()"
        ) else {
            XCTFail(
                "Could not locate `try await downloadManager.bootstrap()` — " +
                "the deferred-bootstrap anchor moved and this canary needs re-anchoring."
            )
            return
        }
        XCTAssertLessThan(
            bgTaskRegistration.upperBound, bootstrap.lowerBound,
            "canary anchors are out of order; re-anchor before trusting the assertion below"
        )

        guard let install = source.range(
            of: "Self.installDayZeroBackgroundDownloadKickoff(",
            range: bgTaskRegistration.upperBound..<bootstrap.lowerBound
        ) else {
            XCTFail(
                """
                The day-0 background-download kickoff observer is NOT installed on the \
                launch path in `PlayheadRuntime.init`. playhead-4dqe installed it from \
                `PlayheadApp.task`, a SwiftUI scene modifier that does not run when iOS \
                relaunches the app headless to drain background URLSession events or to \
                run a BGTask — i.e. exactly when an auto-download completes. In those \
                processes the completion met a nil observer and the day-0 kickoff was \
                dropped with no attempt, no give-up and no `rediff_day_zero_kickoffs` \
                row (playhead-cnql: 16 of 27 post-4dqe assets on the 2026-08-10 pull).
                """
            )
            return
        }
        XCTAssertLessThan(bgTaskRegistration.upperBound, install.lowerBound)

        // The scene attach may install the SwiftData FACTS resolver — an
        // upgrade — but must never be the only thing that installs the observer.
        guard let attachRange = source.range(
            of: "func attachRediffEnclosureResolver(modelContainer: ModelContainer) {"
        ) else {
            XCTFail("Could not locate `attachRediffEnclosureResolver` — re-anchor this canary.")
            return
        }
        guard let braceIndex = source[attachRange.lowerBound...].firstIndex(of: "{") else {
            XCTFail("Could not locate `attachRediffEnclosureResolver`'s body brace.")
            return
        }
        let attachBody = SwiftSourceInspector.bracedBody(in: source, startingAt: braceIndex)
        XCTAssertFalse(attachBody.isEmpty, "ANTI-VACUITY: the attach body must be readable")
        XCTAssertNil(
            attachBody.range(of: "setBackgroundDownloadCompletionObserver"),
            """
            `attachRediffEnclosureResolver` installs the background-download completion \
            observer again. That function runs only from `PlayheadApp.task`, so making it \
            the install site is the playhead-cnql regression.
            """
        )
        XCTAssertNotNil(
            attachBody.range(of: "dayZeroKickoffEpisodeFactsBox.resolver"),
            """
            ANTI-VACUITY: the scene attach must still install the SwiftData facts \
            resolver. Without it every background kickoff would permanently carry the \
            download's own URL and an unknown publish date, and the assertion above \
            would pass on a build that simply deleted the wiring.
            """
        )
    }

    /// playhead-cnql R1 review: the observer being installed at launch buys
    /// nothing unless the COMPLETION can reach it, and in a headless relaunch it
    /// could not.
    ///
    /// `PlayheadAppDelegate` is the only production code that re-instantiates a
    /// background `URLSession` in a relaunched process
    /// (`handleEventsForBackgroundURLSession` → `DownloadManager.shared` →
    /// `resumeSession(identifier:)`), and sessions are created lazily — so with
    /// `shared` unset the delegate never fires,
    /// `handleBackgroundDownloadComplete` never runs, and the launch-installed
    /// observer waits for an event that will never arrive. Both slots were
    /// published only from `PlayheadApp.task`, the same scene modifier this bead
    /// moved the observer off.
    func testDownloadManagerSharedSlotsArePublishedOnTheLaunchPath() throws {
        let runtimeSource = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )
        guard let previewGuard = runtimeSource.range(
            of: "guard !isPreviewRuntime else { return }"
        ) else {
            XCTFail(
                "Could not locate the `isPreviewRuntime` guard — re-anchor this canary."
            )
            return
        }
        guard let bootstrap = runtimeSource.range(
            of: "try await downloadManager.bootstrap()"
        ) else {
            XCTFail(
                "Could not locate `try await downloadManager.bootstrap()` — re-anchor this canary."
            )
            return
        }
        XCTAssertLessThan(
            previewGuard.upperBound, bootstrap.lowerBound,
            "canary anchors are out of order; re-anchor before trusting the assertion below"
        )
        XCTAssertNotNil(
            runtimeSource.range(
                of: "DownloadManager.registerShared(downloadManager)",
                range: previewGuard.upperBound..<bootstrap.lowerBound
            ),
            """
            `PlayheadRuntime.init` no longer publishes the live DownloadManager on the \
            launch path. Its only other call site is `PlayheadApp.task` — a SwiftUI scene \
            modifier — so a headless background-URLSession relaunch would leave \
            `DownloadManager.shared` nil, `resumeSession(identifier:)` uncalled, the \
            background session never re-instantiated, and the day-0 completion this bead \
            exists to deliver never produced at all.
            """
        )

        let delegateSource = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadAppDelegate.swift"
        )
        guard let didFinishLaunching = delegateSource.range(
            of: "didFinishLaunchingWithOptions launchOptions:"
        ) else {
            XCTFail(
                "Could not locate `didFinishLaunchingWithOptions` — re-anchor this canary."
            )
            return
        }
        XCTAssertNotNil(
            delegateSource.range(
                of: "DownloadManager.registerAppDelegate(self)",
                range: didFinishLaunching.upperBound..<delegateSource.endIndex
            ),
            """
            `PlayheadAppDelegate` no longer self-registers at launch. Without it \
            `urlSessionDidFinishEvents(forBackgroundURLSession:)` cannot reach the stored \
            OS completion handler in a headless relaunch, and an app that never invokes \
            that handler is one iOS stops granting background time.
            """
        )
    }
}
