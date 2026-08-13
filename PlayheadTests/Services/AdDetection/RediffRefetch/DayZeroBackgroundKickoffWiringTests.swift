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
//   4. `DayZeroKickoffClaimRecorderTests` (playhead-kg8h) — the production
//      claim closure writes a durable `requested` row.
//   5. `DayZeroKickoffClaimSilenceTests` /
//      `DayZeroBackgroundObserverInstallRecordTests` (playhead-oa82) — the
//      four sites on this chain that used to write NOTHING now say so, because
//      "the claim write failed" and "the request never happened" left a
//      byte-identical database and the 2026-08-12 pull could not tell them
//      apart. See the section-5 header for the full accounting.
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

    /// playhead-oa82: the launch-path block must ALSO record the case where
    /// there is no coordinator to install an observer for.
    ///
    /// Structural for the same reason its sibling above is: nothing in this
    /// suite constructs a real `PlayheadRuntime`, so no behavioural test can
    /// reach either arm of this `if let`. The property is worth a rail anyway,
    /// because the install record is only readable AGAINST ITS OWN ABSENCE — and
    /// an absent install line has two unrelated causes ("there was no
    /// coordinator" and "`init`'s un-awaited install task never ran"). Deleting
    /// the `else` would compile, leave the suite green, and quietly restore the
    /// ambiguity this bead exists to remove.
    func testAnAbsentDayZeroCoordinatorIsRecordedOnTheLaunchPath() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )
        guard let bgTaskRegistration = source.range(
            of: "BackgroundFeedRefreshService.registerTaskHandler()"
        ), let bootstrap = source.range(
            of: "try await downloadManager.bootstrap()"
        ) else {
            XCTFail(
                "Could not locate the launch-path anchors — re-anchor this canary " +
                "before trusting the assertions below."
            )
            return
        }
        XCTAssertLessThan(
            bgTaskRegistration.upperBound, bootstrap.lowerBound,
            "canary anchors are out of order; re-anchor before trusting the assertions below"
        )
        let region = String(source[bgTaskRegistration.upperBound..<bootstrap.lowerBound])
        XCTAssertNotNil(
            region.range(of: "Self.installDayZeroBackgroundDownloadKickoff("),
            """
            ANTI-VACUITY: the install itself must live in this region, or the \
            assertion below would be searching a block that no longer decides \
            anything about the day-0 observer.
            """
        )
        XCTAssertNotNil(
            region.range(of: "rediffDayZeroKickoffCoordinatorAbsent"),
            """
            The launch-path install block no longer records the NO-COORDINATOR \
            case. Its absence is not neutral: on the 2026-08-12 pull the whole \
            diagnosis of `rediff_day_zero_kickoffs = 0` rested on a five-link \
            source inference that the coordinator was non-nil, whose load-bearing \
            link is a flag on `EpisodeFingerprintCapture`. One line makes that \
            answerable from the device instead.
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

    /// playhead-kg8h R2: the coordinator must be built with the REAL claim
    /// recorder.
    ///
    /// `claimKickoff` is a REQUIRED init parameter, which makes an OMITTED claim
    /// a compile error — and does nothing whatsoever about a MIS-WIRED one.
    /// `claimKickoff: { _ in }` compiles, satisfies that discipline, and passes
    /// every coordinator test in the suite, because they all inject their own spy
    /// and none of them touches this closure. It also restores playhead-kg8h's
    /// defect in full: a kickoff the process does not survive leaves NO ROW,
    /// byte-identical in the database to a download that never happened, and a
    /// missing claim is only ever visible as an absence.
    ///
    /// Two instruments, because neither is sufficient alone.
    /// `DayZeroKickoffClaimRecorderTests` drives what
    /// `makeDayZeroKickoffClaimRecorder` RETURNS against a real store — proving
    /// the closure writes the row, with the claim's own source and stamp. This
    /// canary proves the coordinator is actually built with it, which no
    /// behavioural test can see without standing up the whole runtime.
    func testDayZeroKickoffCoordinatorIsWiredToTheRealClaimRecorder() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )

        // ANTI-VACUITY: both anchors must exist and be in order, or the bounded
        // search below would silently run over the wrong region — or over none.
        guard let construction = source.range(
            of: "RediffDayZeroKickoffCoordinator("
        ) else {
            XCTFail(
                "Could not locate the `RediffDayZeroKickoffCoordinator(` construction — " +
                "re-anchor this canary."
            )
            return
        }
        guard let lastArgument = source.range(
            of: "episodeIdHasher: surfaceStatusHasher",
            range: construction.upperBound..<source.endIndex
        ) else {
            XCTFail(
                "Could not locate `episodeIdHasher: surfaceStatusHasher`, the coordinator's " +
                "final argument — re-anchor this canary."
            )
            return
        }

        let argumentList = SwiftSourceInspector.strippingComments(
            String(source[construction.upperBound..<lastArgument.lowerBound])
        )
        XCTAssertFalse(
            argumentList.isEmpty,
            "ANTI-VACUITY: the coordinator's argument list must be readable"
        )
        XCTAssertNotNil(
            argumentList.range(of: "recordKickoff:"),
            """
            ANTI-VACUITY: the argument list must contain the coordinator's OTHER \
            recorder. Without this the assertion below could pass over a region that \
            is empty or mis-bounded.
            """
        )

        XCTAssertNotNil(
            argumentList.range(of: "claimKickoff: Self.makeDayZeroKickoffClaimRecorder("),
            """
            The day-0 kickoff coordinator is no longer built with the real claim \
            recorder. `claimKickoff` being a REQUIRED parameter stops the wiring being \
            OMITTED; it says nothing about it being replaced with `{ _ in }`, or with \
            any other closure that never reaches `rediff_day_zero_kickoffs`. Either \
            compiles and leaves the whole suite green — every coordinator test injects \
            its own spy — and restores playhead-kg8h's defect: every kickoff the \
            process does not survive leaves nothing behind.
            """
        )
    }

    /// playhead-kg8h R4: the claim must be awaited INLINE — and that is a
    /// STRUCTURAL question the behavioural rails cannot settle.
    ///
    /// `DayZeroKickoffClaimRecorderTests` proves the row lands. It cannot prove
    /// the row lands BEFORE the recorder returned, because a detached write
    /// differs from the shipped closure only in SCHEDULING: with the store call
    /// wrapped in an unstructured task, those tests fail solely when the test's
    /// own hop into `AnalysisStore` beats the detached task's. That reddened on
    /// every observation R3 took — and a rail that CAN flake green is the
    /// dangerous direction, because green is how it reports "no regression".
    /// This is the same box whose measurement tests blow 60 s budgets under the
    /// full plan's own load, so "it reddened on a quiet box" is not evidence
    /// about a loaded one.
    ///
    /// Detachment is a defect here and not a matter of taste. `requestKickoff`
    /// appends to `pending` and starts the drain on the line AFTER
    /// `await claimKickoff(…)`. A write that is merely SCHEDULED is one the
    /// drain's own `settle` can overtake — it would then stamp `.requested` onto
    /// an already-settled row and add a kickoff nobody owes, which is exactly the
    /// ordering `noClaimIsOvertakenByItsOwnSettle` rails one layer up — and, in
    /// the background-relaunch process this bead exists for, it is a write the
    /// process may not survive long enough to perform at all.
    ///
    /// Inline-ness is textual, so this reads it textually. Every way Swift has of
    /// not awaiting something here — an unstructured task, a detached task, a
    /// GCD hop, a fire-and-forget helper — puts the call inside a closure
    /// literal. So the property is: the store write sits at closure depth 1,
    /// inside the returned closure and inside nothing else. Control-flow braces
    /// are NOT counted (see `closureDepths`), so a later `do { } catch { }`
    /// around the same inline `await` still reads 1.
    ///
    /// WHAT WOULD MAKE THIS COMPILE-ENFORCED, AND WHY IT IS NOT DONE HERE.
    /// `claimKickoff` could return a receipt that only
    /// `AnalysisStore.noteRediffDayZeroKickoffClaim` is able to construct, so a
    /// closure that detached would have nothing to return. That changes the store
    /// method's signature, the coordinator's seam type and `requestKickoff` — an
    /// architecture change rather than a rail — and it would still leak, because
    /// the recorder deliberately swallows store errors with `try?` and therefore
    /// has to return an optional, which a mutant satisfies with `nil`.
    func testDayZeroKickoffClaimRecorderAwaitsItsWriteInline() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )
        guard let body = SwiftSourceInspector.firstBody(
            in: source,
            after: "static func makeDayZeroKickoffClaimRecorder("
        ) else {
            XCTFail(
                "Could not locate `makeDayZeroKickoffClaimRecorder`'s body — the day-0 " +
                "claim recorder's factory moved or was renamed and this canary needs " +
                "re-anchoring."
            )
            return
        }

        // Comments AND string CONTENTS blanked: the prose above quotes the
        // detached spelling, and a canary that greps its own explanation is a
        // canary that is always red.
        let code = SwiftSourceInspector.strippingCommentsAndStrings(body)
        XCTAssertFalse(
            code.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
            "ANTI-VACUITY: the factory's body must be readable"
        )

        let depths = Self.closureDepths(of: "noteRediffDayZeroKickoffClaim(", in: code)
        XCTAssertFalse(
            depths.isEmpty,
            """
            ANTI-VACUITY: `makeDayZeroKickoffClaimRecorder` no longer calls \
            `noteRediffDayZeroKickoffClaim` at all. The depth assertion below is \
            vacuously satisfiable by a factory that writes nothing — which IS \
            playhead-kg8h's defect — so restore the write or re-anchor this canary.
            """
        )
        XCTAssertEqual(
            depths, [1],
            """
            The day-0 claim is no longer awaited INLINE in \
            `makeDayZeroKickoffClaimRecorder`: `noteRediffDayZeroKickoffClaim` occurs \
            at closure depth(s) \(depths) rather than exactly [1], i.e. inside a \
            nested closure. An unstructured task, a detached task and a GCD hop all \
            read this way, and all three turn the claim into a write that \
            `requestKickoff`'s own `pending.append` and drain can overtake — and one \
            a background-relaunch process may never perform. \
            `DayZeroKickoffClaimRecorderTests` cannot catch that reliably: against a \
            detached write it fails only when the test wins a scheduling race. If you \
            restructured this body deliberately, check FIRST that the await is still \
            on the closure's own execution path.
            """
        )
    }

    /// playhead-oa82 (R1 review): the ATTEMPT line must be emitted BEFORE the
    /// store write, not merely alongside it.
    ///
    /// This is the one property the claim PAIR exists for, and no behavioural
    /// test in the repo can reach it. `DayZeroKickoffClaimSilenceTests` asserts
    /// the emitted CODES — `[.attempted]` on the healthy path, `[.attempted,
    /// .writeFailed]` on a throw — and both sequences are reproduced exactly by
    /// a recorder that captures the write's outcome first and reports afterwards:
    ///
    ///     var thrown: Error?
    ///     do { try await store.note…() } catch { thrown = error }
    ///     await reportViolation(.rediffDayZeroKickoffClaimAttempted, …)
    ///     if let thrown { await reportViolation(.…ClaimWriteFailed, …) }
    ///
    /// R1 ran exactly that mutant: every suite here stayed green (10 Swift
    /// Testing tests + 5 canaries, exit 0). Under it a store write that PARKS —
    /// state (b), the reason the instrument is a pair and not the single `else`
    /// the bead description asked for — emits NOTHING, which is byte-identical
    /// to "the request never happened" and restores the ambiguity this bead
    /// exists to remove.
    ///
    /// A behavioural test would need to inject a parking store, and the factory
    /// takes a concrete `AnalysisStore`, so there is no seam to inject through.
    /// Hence textual, on the same reasoning (and with the same anti-vacuity
    /// discipline) as `testDayZeroKickoffClaimRecorderAwaitsItsWriteInline`
    /// directly above.
    func testDayZeroKickoffClaimAttemptIsRecordedBeforeTheStoreWrite() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )
        guard let body = SwiftSourceInspector.firstBody(
            in: source,
            after: "static func makeDayZeroKickoffClaimRecorder("
        ) else {
            XCTFail(
                "Could not locate `makeDayZeroKickoffClaimRecorder`'s body — the day-0 " +
                "claim recorder's factory moved or was renamed and this canary needs " +
                "re-anchoring."
            )
            return
        }
        // Comments and string CONTENTS blanked, for the reason the sibling
        // canary blanks them: the doc comment above quotes both spellings, and
        // the description literals name the codes too.
        let code = SwiftSourceInspector.strippingCommentsAndStrings(body)

        guard let attempt = code.range(of: ".rediffDayZeroKickoffClaimAttempted"),
              let write = code.range(of: "noteRediffDayZeroKickoffClaim(") else {
            XCTFail(
                """
                ANTI-VACUITY: the factory must contain BOTH the attempt report and \
                the store write. Missing either makes the ordering assertion below \
                vacuous — and a factory with no attempt report is precisely the \
                pre-oa82 defect, while one with no write is playhead-kg8h's.
                """
            )
            return
        }
        XCTAssertEqual(
            SwiftSourceInspector.occurrences(
                of: ".rediffDayZeroKickoffClaimAttempted", in: code
            ),
            1,
            """
            ANTI-VACUITY: the attempt code must appear exactly once. Two \
            occurrences would let one of them sit before the write and satisfy \
            the ordering assertion below while the reachable one does not.
            """
        )
        XCTAssertLessThan(
            attempt.lowerBound, write.lowerBound,
            """
            The day-0 claim ATTEMPT is no longer reported BEFORE the store write. \
            That ordering is the whole reason this is a PAIR rather than the single \
            failure path the bead description asked for: `try?`/`catch` fires on a \
            THROW, but `RediffDayZeroKickoffCoordinator.requestKickoff` documents, \
            as accepted residue, that this write can instead PARK on a wedged \
            `AnalysisStore` and never return — no row, no throw, no failure line. \
            Only a line written BEFORE the call survives that, and it is what makes \
            "attempted + neither" readable as the park. Reporting afterwards \
            reproduces every code sequence this suite asserts while silently \
            deleting state (b); R1 shipped that mutant and the suite stayed green.
            """
        )
    }

    /// Closure-literal nesting depths at which `needle` occurs in `code`.
    ///
    /// A `{` counts only when it opens a CLOSURE (or a nested function) — that
    /// is, when the text back to the previous `{`, `}` or `;` contains no Swift
    /// control-flow keyword. A trailing closure on a task, a queue or any other
    /// call counts; `do`, `if`, `guard`, `else`, `for`, `while`, `repeat`,
    /// `switch`, `case`, `default`, `catch` and `defer` do not, so wrapping the
    /// same inline `await` in `do { } catch { }` does not red the canary.
    ///
    /// The bias is deliberate and one-directional: an unrecognised construct
    /// carries no keyword, so it reads as a closure and FAILS the depth check. A
    /// rail that errs loud is repairable; one that errs quiet is the thing this
    /// exists to replace.
    ///
    /// `code` must already have had comments and string contents blanked with
    /// `SwiftSourceInspector.strippingCommentsAndStrings`, so this walker only
    /// has to track braces.
    private static func closureDepths(of needle: String, in code: String) -> [Int] {
        let controlFlowKeywords: Set<String> = [
            "do", "if", "else", "guard", "for", "while", "repeat",
            "switch", "case", "default", "catch", "defer"
        ]
        var depths: [Int] = []
        var closureDepth = 0
        /// One entry per open brace: `true` when it was counted as a closure, so
        /// its `}` decrements only what its `{` incremented.
        var braceStack: [Bool] = []
        var statementStart = code.startIndex
        var index = code.startIndex

        while index < code.endIndex {
            if code[index...].hasPrefix(needle) {
                depths.append(closureDepth)
                index = code.index(index, offsetBy: needle.count)
                continue
            }
            switch code[index] {
            case "{":
                let preceding = code[statementStart..<index]
                let words = preceding.split(whereSeparator: { !$0.isLetter })
                let opensClosure = !words.contains { controlFlowKeywords.contains(String($0)) }
                braceStack.append(opensClosure)
                if opensClosure { closureDepth += 1 }
                statementStart = code.index(after: index)
            case "}":
                if braceStack.popLast() == true { closureDepth -= 1 }
                statementStart = code.index(after: index)
            case ";":
                statementStart = code.index(after: index)
            default:
                break
            }
            index = code.index(after: index)
        }
        return depths
    }
}

// MARK: - 4. The production claim recorder (playhead-kg8h)

@Suite("Day-0 kickoff claim recorder — the PRODUCTION closure (playhead-kg8h)")
struct DayZeroKickoffClaimRecorderTests {

    @Test("""
    THE PRODUCTION CLOSURE WRITES THE ROW: `makeDayZeroKickoffClaimRecorder` \
    lands a durable `requested` row carrying the claim's own source and stamp
    """)
    func productionClaimRecorderWritesADurableRow() async throws {
        let store = try await makeTestStore()
        let record = PlayheadRuntime.makeDayZeroKickoffClaimRecorder(
            store: store,
            reportViolation: { _, _ in },
            hashEpisodeId: { _ in oa82OpaqueHash }
        )

        await record(RediffDayZeroKickoffClaim(
            episodeId: "ep-wired", source: .downloadAndAnalyzeTap, at: 1_722_000_042
        ))

        let row = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-wired"))
        #expect(row.lastOutcome == .requested)
        #expect(row.kickoffCount == 1)
        #expect(row.firedCount == 0)
        #expect(row.gaveUpCount == 0)
        // EVERY field, because a mis-wiring drops them one at a time and each
        // loss is silent. A dropped `source` makes a tap indistinguishable from a
        // background download — the one thing this ledger exists to tell a
        // support engineer. A dropped `at` stamps the row 1970, which sorts it
        // last in `idx_rediff_day_zero_kickoffs_updated`, so the kickoffs still
        // owed become exactly the rows a limited pull drops first.
        #expect(row.lastSource == .downloadAndAnalyzeTap)
        #expect(row.updatedAt == 1_722_000_042)
    }

    @Test("""
    the recorder is idempotent per REQUEST, not per episode — a retry download \
    or a tap after a background attempt is a SECOND kickoff and counts
    """)
    func productionClaimRecorderCountsEachRequest() async throws {
        let store = try await makeTestStore()
        let record = PlayheadRuntime.makeDayZeroKickoffClaimRecorder(
            store: store,
            reportViolation: { _, _ in },
            hashEpisodeId: { _ in oa82OpaqueHash }
        )

        await record(RediffDayZeroKickoffClaim(
            episodeId: "ep-twice", source: .backgroundDownload, at: 100
        ))
        await record(RediffDayZeroKickoffClaim(
            episodeId: "ep-twice", source: .downloadAndAnalyzeTap, at: 200
        ))

        let row = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-twice"))
        #expect(row.kickoffCount == 2, """
            Collapsing two genuine requests into one row-with-one-count would hide the \
            second kickoff entirely — and it is the second one, arriving after a failed \
            background attempt, that a support engineer is most likely to be chasing.
            """)
        #expect(row.kickoffCount - (row.firedCount + row.gaveUpCount) == 2,
                "both are owed: neither has settled")
        #expect(row.lastSource == .downloadAndAnalyzeTap)
        #expect(row.updatedAt == 200)
    }
}

// MARK: - 5. The silences the ledger could not explain (playhead-oa82)
//
// THE OBSERVATION. On the 2026-08-12 pull `rediff_day_zero_kickoffs` held ZERO
// rows on a VIRGIN database where four episodes had been downloaded — and the
// ledger could not say why, because BOTH of its writers go through `try?`. "The
// claim write failed" and "`requestKickoff` never ran" produce a BYTE-IDENTICAL
// database: the only writer that would have recorded either is the one that
// failed. The ledger built to make a lost kickoff visible had a silent
// total-failure mode of its own, and it is the only ledger whose complete
// absence is unexplainable.
//
// This is the anchor supply for the whole auto-skip path, which is why it is
// worth this much rail. `AutoSkipEdgeAnchor` has three cases, so only the
// stinger refiner and rediff can anchor an edge; on the device the refiner
// snapped 5 END edges and 0 START edges, so `.rediffByteExact` is the only
// anchor kind that gets a span past playhead-2350's extent gate. No day-0
// kickoff ⇒ no rediff window ⇒ every span unanchored at the start ⇒ markOnly.
//
// FOUR NEW LINES, EACH AT A SITE THAT PREVIOUSLY WROTE NOTHING:
//   * the claim ATTEMPT, written before the store call — the only evidence that
//     survives a write which neither lands nor throws;
//   * the claim FAILURE, when it throws;
//   * the background observer INSTALL, with the buffered completions it drained;
//   * a background completion DROPPED for want of any URL.
//
// Together they separate four states that were previously one absence. The
// attempt line is deliberately written on the HEALTHY path too, on the
// `adWindowIngestCensus` precedent and for its reason: only a line that is
// always present can distinguish "it never ran" from "it ran and produced
// nothing".

/// The reporter seam under test, plus the hasher discipline. A hasher that does
/// NOT embed its input is used on purpose: it lets a test assert that the RAW
/// episode id never reaches the stream, which a `"hash(\(id))"` spy could never
/// show.
private let oa82OpaqueHash = "OPAQUE-EPISODE-HASH"

@Suite("Day-0 kickoff claim records its ATTEMPT, not only its failure (playhead-oa82)")
struct DayZeroKickoffClaimSilenceTests {

    private static func makeRecorder(
        store: AnalysisStore,
        spy: KickoffSpy
    ) -> @Sendable (RediffDayZeroKickoffClaim) async -> Void {
        PlayheadRuntime.makeDayZeroKickoffClaimRecorder(
            store: store,
            reportViolation: { code, description in
                await spy.noteViolation(code: code, description: description)
            },
            hashEpisodeId: { _ in oa82OpaqueHash }
        )
    }

    @Test("""
    A CLAIM THAT LANDS STILL SAYS SO: the healthy path writes the attempt line \
    as well as the row, and writes NO failure line
    """)
    func aSuccessfulClaimRecordsItsAttemptAndNoFailure() async throws {
        let store = try await makeTestStore()
        let spy = KickoffSpy()
        let record = Self.makeRecorder(store: store, spy: spy)

        await record(RediffDayZeroKickoffClaim(
            episodeId: "oa82-lands", source: .backgroundDownload, at: 1_723_000_000
        ))

        // The row is the ANTI-VACUITY control: without it this suite would pass
        // against a recorder that logs and never writes, which is a strictly
        // worse defect than the one it is instrumenting.
        let row = try #require(
            try await store.fetchRediffDayZeroKickoff(episodeId: "oa82-lands")
        )
        #expect(row.kickoffCount == 1)
        #expect(row.lastOutcome == .requested)

        let codes = await spy.violations.map(\.code)
        #expect(codes == [.rediffDayZeroKickoffClaimAttempted], """
            The attempt must be recorded on the HEALTHY path too. A failure-only \
            instrument cannot separate "the write threw" from "the write never \
            returned" from "the request never happened" — all three leave the same \
            absence, which is the defect this bead exists to remove.
            """)
    }

    @Test("""
    the attempt line names the REQUEST PATH and carries a HASHED episode id — \
    the raw id never reaches the diagnostics stream
    """)
    func theAttemptLineCarriesTheSourceAndAHashedId() async throws {
        let store = try await makeTestStore()
        let spy = KickoffSpy()
        let record = Self.makeRecorder(store: store, spy: spy)

        await record(RediffDayZeroKickoffClaim(
            episodeId: "oa82-tap-episode", source: .downloadAndAnalyzeTap, at: 42
        ))

        let description = try #require(await spy.violations.first?.description)
        #expect(description.contains(RediffDayZeroKickoffSource.downloadAndAnalyzeTap.rawValue), """
            Which path asked is the first question a reader of this line has, and \
            the two paths have materially different numbers of hops in front of them.
            """)
        #expect(description.contains(oa82OpaqueHash))
        #expect(!description.contains("oa82-tap-episode"), """
            The raw episode id must not reach the JSON Lines stream — the same \
            hashing discipline every other producer on that stream follows.
            """)
    }

    @Test("""
    THE H1 DETECTOR: a claim write that THROWS is recorded rather than swallowed \
    — and the recorder still returns, because a throw on a URLSession completion \
    callback is its own hazard
    """)
    func aFailedClaimWriteIsRecordedRatherThanSwallowed() async throws {
        let store = try await makeTestStore()
        // The exact shape H1 postulates, and the cheapest reproduction of it:
        // the statement cannot be prepared, so the write throws where `try?` used
        // to eat it. A wedged store actor and a locked database arrive at the
        // same call site by a different route.
        try await store.execForTesting("DROP TABLE rediff_day_zero_kickoffs")

        let spy = KickoffSpy()
        let record = Self.makeRecorder(store: store, spy: spy)

        await record(RediffDayZeroKickoffClaim(
            episodeId: "oa82-throws", source: .backgroundDownload, at: 7
        ))

        let codes = await spy.violations.map(\.code)
        #expect(codes == [
            .rediffDayZeroKickoffClaimAttempted,
            .rediffDayZeroKickoffClaimWriteFailed
        ], """
            A claim write that throws must leave a line naming itself. Before this \
            bead the whole failure was `try?` with no `else`, so a device on which \
            every claim threw was byte-identical to one on which no kickoff was ever \
            requested — which is exactly the 2026-08-12 pull.
            """)
        let failure = try #require(
            await spy.violations.last(where: {
                $0.code == .rediffDayZeroKickoffClaimWriteFailed
            })?.description
        )
        #expect(failure.contains(RediffDayZeroKickoffSource.backgroundDownload.rawValue))
        #expect(failure.contains(oa82OpaqueHash))
        #expect(!failure.contains("oa82-throws"))
        #expect(failure.count > oa82OpaqueHash.count + 40, """
            ANTI-VACUITY: the thrown error must actually be interpolated into the \
            description. `no such table` and `database is locked` are different bugs \
            with different owners, and a line that names neither sends the next \
            reader nowhere.
            """)
    }
}

@Suite("Day-0 background observer records that it was INSTALLED (playhead-oa82)")
struct DayZeroBackgroundObserverInstallRecordTests {

    private func makeManager(in dir: URL) async throws -> DownloadManager {
        let manager = DownloadManager(
            cacheDirectory: dir,
            workJournalRecorder: KickoffHandoffJournal()
        )
        try await manager.bootstrap()
        return manager
    }

    /// A coordinator whose only job here is to say whether a request reached it.
    private func makeCoordinator(spy: KickoffSpy) -> RediffDayZeroKickoffCoordinator {
        RediffDayZeroKickoffCoordinator(
            maxAttempts: 1,
            pollNanos: 1,
            probe: { _ in .awaitingPinnedFile },
            fire: { _, _ in },
            claimKickoff: { await spy.noteClaim($0) },
            recordKickoff: { await spy.noteRecord($0) },
            reportViolation: { _, _ in },
            episodeIdHasher: { _ in oa82OpaqueHash },
            sleep: { _ in },
            now: { 1_000 }
        )
    }

    private func install(
        manager: DownloadManager,
        coordinator: RediffDayZeroKickoffCoordinator,
        factsBox: DayZeroKickoffEpisodeFactsBox,
        reports: KickoffSpy
    ) async {
        await PlayheadRuntime.installDayZeroBackgroundDownloadKickoff(
            downloads: manager,
            coordinator: coordinator,
            factsBox: factsBox,
            reportViolation: { code, description in
                await reports.noteViolation(code: code, description: description)
            },
            hashEpisodeId: { _ in oa82OpaqueHash }
        )
    }

    @Test("""
    THE H2 DETECTOR: the install writes a line AFTER the await returns, so "the \
    observer was never installed" stops being indistinguishable from "the claim \
    threw" and from "the request never happened"
    """)
    func theInstallRecordsThatItCompleted() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = try await makeManager(in: dir)
        let reports = KickoffSpy()

        await install(
            manager: manager,
            coordinator: makeCoordinator(spy: KickoffSpy()),
            factsBox: DayZeroKickoffEpisodeFactsBox(),
            reports: reports
        )

        let installs = await reports.violations.filter {
            $0.code == .rediffDayZeroBackgroundObserverInstalled
        }
        #expect(installs.count == 1, """
            This is the leg with the most un-awaited hops in front of it — `init`'s \
            `Task {}`, the actor await, then the observer's own `Task {}` — and it \
            served three of the four assets on the 2026-08-12 pull. Its absence was \
            unattributable.
            """)
        #expect(installs.first?.description.contains("drained 0") == true, """
            An install that found an EMPTY buffer is a different story from one that \
            replayed completions, and the count is the only evidence of which side \
            of the race the install landed on.
            """)
    }

    @Test("""
    the install record counts the buffered completions it DRAINED, and those \
    completions really do reach the coordinator
    """)
    func theInstallRecordCountsWhatItDrained() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = try await makeManager(in: dir)
        let reports = KickoffSpy()
        let claims = KickoffSpy()

        for index in 0..<2 {
            await manager._bufferBackgroundDownloadCompletionForTesting(
                episodeId: "oa82-buffered-\(index)",
                sourceURL: URL(string: "https://example.com/oa82-\(index).mp3")
            )
        }

        await install(
            manager: manager,
            coordinator: makeCoordinator(spy: claims),
            factsBox: DayZeroKickoffEpisodeFactsBox(),
            reports: reports
        )

        let installLine = try #require(
            await reports.violations.first(where: {
                $0.code == .rediffDayZeroBackgroundObserverInstalled
            })?.description
        )
        #expect(installLine.contains("drained 2"))

        // ANTI-VACUITY: the number must describe completions that actually went
        // somewhere. A drained count that no kickoff followed would be a worse
        // lie than no count at all.
        #expect(await pollUntil { await claims.claims.count == 2 }, """
            The drained completions must reach `requestKickoff` and claim. Otherwise \
            the install line reports a hand-off that did not happen.
            """)
    }

    @Test("""
    A COMPLETION WITH NO URL ANYWHERE IS RECORDED, NOT DROPPED SILENTLY: without \
    this line, a URL-less completion reads exactly like an observer that was \
    never installed
    """)
    func aCompletionWithNoURLRecordsItsOwnDrop() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = try await makeManager(in: dir)
        let reports = KickoffSpy()
        let claims = KickoffSpy()

        // No facts resolver (the background-relaunch normal state) AND no
        // download URL: `DayZeroBackgroundKickoff.request` returns nil.
        await manager._bufferBackgroundDownloadCompletionForTesting(
            episodeId: "oa82-urlless",
            sourceURL: nil
        )

        await install(
            manager: manager,
            coordinator: makeCoordinator(spy: claims),
            factsBox: DayZeroKickoffEpisodeFactsBox(),
            reports: reports
        )

        #expect(await pollUntil {
            await reports.violations.contains { $0.code == .rediffDayZeroBackgroundKickoffNoURL }
        }, """
            The `guard let request … else { return }` inside the observer was the \
            third silence on this leg. It sits between the other two instruments: \
            the install line proves the observer exists, this line proves the \
            completion arrived, and the missing claim is then EXPLAINED rather than \
            merely observed.
            """)
        let drop = try #require(
            await reports.violations.first(where: {
                $0.code == .rediffDayZeroBackgroundKickoffNoURL
            })?.description
        )
        #expect(drop.contains(oa82OpaqueHash))
        #expect(!drop.contains("oa82-urlless"))
        #expect(await claims.claims.isEmpty, "no claim can be made for a kickoff with no URL")
    }

    @Test("""
    ANTI-VACUITY CONTROL: a completion that DOES carry a URL takes the normal \
    path — it claims, and writes no drop line
    """)
    func aCompletionWithAURLClaimsAndWritesNoDropLine() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = try await makeManager(in: dir)
        let reports = KickoffSpy()
        let claims = KickoffSpy()

        await manager._bufferBackgroundDownloadCompletionForTesting(
            episodeId: "oa82-has-url",
            sourceURL: URL(string: "https://example.com/oa82-has-url.mp3")
        )

        await install(
            manager: manager,
            coordinator: makeCoordinator(spy: claims),
            factsBox: DayZeroKickoffEpisodeFactsBox(),
            reports: reports
        )

        #expect(await pollUntil { await claims.claims.count == 1 })
        #expect(await claims.claims.first?.source == .backgroundDownload)
        let drops = await reports.violations.filter {
            $0.code == .rediffDayZeroBackgroundKickoffNoURL
        }
        #expect(drops.isEmpty)
    }

    @Test("""
    the DownloadManager seam reports how many completions the install drained — \
    the number the install record is built from
    """)
    func theSetterReturnsTheDrainedCount() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = try await makeManager(in: dir)

        for index in 0..<3 {
            await manager._bufferBackgroundDownloadCompletionForTesting(
                episodeId: "oa82-count-\(index)",
                sourceURL: URL(string: "https://example.com/count-\(index).mp3")
            )
        }
        let drained = await manager.setBackgroundDownloadCompletionObserver { _, _ in }
        #expect(drained == 3)

        // A SECOND install finds an empty buffer — the count is what THIS install
        // drained, not a running total.
        let again = await manager.setBackgroundDownloadCompletionObserver { _, _ in }
        #expect(again == 0)
    }
}
