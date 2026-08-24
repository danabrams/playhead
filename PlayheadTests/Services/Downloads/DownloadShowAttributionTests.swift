// DownloadShowAttributionTests.swift
// playhead-kkzu: a background download must carry the show it belongs to.
//
// The defect: `handleBackgroundDownloadComplete` — the only thing that
// enqueues analysis for a background/auto download — passed `context: nil`
// unconditionally, so `analysis_jobs.podcastId` was NULL for every episode
// that was NOT explicitly played. That is precisely the population whose
// analysis should already be finished when the user presses play, and
// `AnalysisWorkScheduler` turns the NULL into `job.podcastId ?? ""`, pooling
// every unattributed episode under one fake show.
//
// playhead-le02 idiom: every negative assertion here is paired with a
// POSITIVE WITNESS. "podcastId is not nil" is worthless if no job row was
// written at all, and "podcastId is nil for an unattributed download" is
// worthless if the assertion would hold for an attributed one too. Each rail
// below drives BOTH arms through the same harness.
//
// playhead-et2d: EVERY `DownloadManager` HERE IS BUILT BY `makeManager`,
// which injects `unsharedSessionIO()` and asserts the queue it got is not the
// shared one. The reason is one measurement rather than tidiness. The default
// `BackgroundSessionIO.shared` is a process-wide singleton with ONE serial
// queue. On the full-plan run of 2026-08-23 08:06 THIS SUITE'S OWN
// `downloadTask(with:) for kkzu-cleared` parked inside `nsurlsessiond` and
// held that queue for 65 seconds; seventeen later submissions — twelve
// distinct transfers across THREE suites (this one, `StreamingDownloadTests`,
// `ForceQuitResumeTests`) — then arrived on one thread, microseconds apart, in
// submission order, logging "reached the daemon queue after its caller had
// already given up — not started". All seven tests in this file that build a
// manager failed together. They failed as ASSERTIONS, which is why the seven
// baseline entries were recorded that way, and why the recorded KIND said
// nothing about the cause: every refusal branch in `backgroundDownload`
// DELETES the attribution sidecar, so this suite reads a missing show and
// blames the code that writes it.
//
// The helper keeps the real daemon and the production bound and changes only
// the queue label — see `unsharedSessionIO` for what that does and does not
// claim; in particular it does NOT make a call immune to a slow daemon.
// `.neverAnswers` (playhead-7wia's fix for the other three victims) cannot be
// used here: these tests need a genuinely ADMITTED transfer, because the
// sidecar they assert on is what a refusal destroys.

import Foundation
import Testing
@testable import Playhead

@Suite("Download show attribution (playhead-kkzu)")
struct DownloadShowAttributionTests {

    private static let showId = "https://feeds.example.com/diary.xml"

    /// Builds this suite's `DownloadManager` and PROVES, before handing it
    /// back, that it is not on the process-wide `BackgroundSessionIO` queue.
    ///
    /// playhead-et2d. The injection is the whole fix, and without this rail
    /// the only thing standing between "every manager here has its own queue"
    /// and "one of them quietly went back to `.shared`" would be eight
    /// identical argument lists nobody re-reads. A construction is a cheap
    /// thing to copy-paste wrongly, and the consequence — a test that fails
    /// only under a full plan's own concurrency, and only when some OTHER
    /// suite parks the daemon — is the most expensive kind to diagnose: it
    /// reads as this file's defect and it is not.
    ///
    /// WHAT IT CANNOT SEE, so a green run is read for what it is worth. It
    /// checks ONE label against ONE constant, so it says nothing about (a) a
    /// `DownloadManager` built anywhere in this file WITHOUT this helper —
    /// nothing enforces that route; (b) a custom label that is nevertheless
    /// SHARED between managers, which `unsharedSessionIO`'s `UUID` rules out
    /// today but this assertion would not notice; or (c) two calls made
    /// through ONE manager, which share that manager's single queue by
    /// construction and are unaffected by any of this.
    ///
    /// `#function` is evaluated at the CALL SITE, so each manager's queue is
    /// still labelled with the test that built it.
    private func makeManager(
        cacheDirectory dir: URL,
        labelledFor test: String = #function,
        sourceLocation: SourceLocation = #_sourceLocation
    ) async -> DownloadManager {
        let manager = DownloadManager(
            cacheDirectory: dir,
            sessionIO: unsharedSessionIO(labelledFor: test)
        )
        let io = await manager.sessionIO
        #expect(
            io.queueLabel != BackgroundSessionIO.defaultQueueLabel,
            """
            this manager is on the process-wide BackgroundSessionIO queue \
            (\(io.queueLabel)) — every assertion below is one parked \
            downloadTask(with:) in an unrelated suite away from failing, \
            which is what playhead-et2d fixed
            """,
            sourceLocation: sourceLocation
        )
        return manager
    }

    private func makeScheduler(store: AnalysisStore) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: speechService,
                store: store
            ),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig()
        )
    }

    /// Drives one background completion to its analysis enqueue and returns
    /// the job row that landed. `manager` is injectable so a rail can hand
    /// the completion to a DIFFERENT manager than the one that queued the
    /// transfer — the process-restart case.
    @discardableResult
    private func completeBackgroundDownload(
        manager: DownloadManager,
        store: AnalysisStore,
        episodeId: String,
        cacheDir: URL
    ) async throws -> AnalysisJob? {
        let staged = cacheDir.appendingPathComponent("\(episodeId)-staged.mp3")
        // Distinct bytes per episode: the scheduler's `workKey` is derived
        // from the source fingerprint, so two episodes staged with identical
        // content would collide and the second enqueue would be dropped as a
        // duplicate — silently making a contrast arm look like a failure to
        // enqueue.
        try Data("kkzu-\(episodeId)-".utf8).write(to: staged)
        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: staged,
            originalURL: URL(string: "https://cdn.example.com/\(episodeId).mp3"),
            metadata: HTTPAssetMetadata(
                etag: "\"kkzu\"", contentLength: 512, lastModified: nil
            )
        )
        return try await store.fetchLatestJobForEpisode(episodeId)
    }

    // MARK: - R1 (centrepiece): the enqueue carries the show

    /// THE rail this bead exists for. A background download queued with a
    /// show must enqueue its analysis job against that show.
    ///
    /// Both witnesses are load-bearing:
    ///   - a job row EXISTS, so "podcastId != nil" is not vacuously true
    ///     because nothing was enqueued;
    ///   - the SAME harness driven with an unattributed context produces a
    ///     job whose podcastId IS nil, so the assertion is sensitive to the
    ///     context rather than to some unrelated default.
    @Test("A background download enqueues analysis against the show it was queued with")
    func backgroundDownloadEnqueuesWithPodcastId() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeTestStore()
        let manager = await makeManager(cacheDirectory: dir)
        await manager.setAnalysisWorkScheduler(makeScheduler(store: store))
        try await manager.bootstrap()

        let attributed = "kkzu-attributed"
        await manager.backgroundDownload(
            episodeId: attributed,
            from: URL(string: "https://cdn.example.com/\(attributed).mp3")!,
            context: DownloadContext(
                podcastId: Self.showId,
                isExplicitDownload: false,
                podcastTitle: "Diary of a CEO",
                episodeTitle: "Episode 1"
            )
        )
        let attributedJob = try await completeBackgroundDownload(
            manager: manager, store: store,
            episodeId: attributed, cacheDir: dir
        )

        // Positive witness: a job actually landed.
        #expect(
            attributedJob != nil,
            "no analysis job was enqueued at all — the podcastId assertion below would be vacuous"
        )
        #expect(
            attributedJob?.podcastId == Self.showId,
            "a background download must record the show it belongs to, got \(String(describing: attributedJob?.podcastId))"
        )
        #expect(attributedJob?.podcastId?.isEmpty == false)

        // Contrast arm, same harness: an UNATTRIBUTED context still enqueues,
        // and still records no show. Without this the rail above could pass on
        // a harness that hard-codes a podcastId somewhere.
        //
        // playhead-et2d: THE SIDECAR IS WRITTEN DIRECTLY, not through
        // `backgroundDownload`, and this arm is the only one in the file that
        // needs that. `forceQuitResumeRecoversTheShow` below already uses the
        // idiom; here the reason is that this arm ASSERTS AN ABSENCE and a
        // refused transfer produces the same absence. All three no-answer
        // branches of `backgroundDownload` delete the sidecar, and a completion
        // that finds none defaults to `.unattributed(.resumeWithoutRecordedShow)`
        // — byte-identical to the context this arm passes. So
        // `unattributedJob?.podcastId == nil` reads the same whether the
        // download was admitted or never started: this repo's standing defect
        // class sitting inside the assertion.
        //
        // MEASURED, which is why the arm was moved off the daemon rather than
        // given a witness: over the 57 de-duplicated full-plan logs of
        // 2026-08-13 … 08-24, `Background download for kkzu-unattributed NOT
        // started: the background transfer daemon did not answer` appears in
        // 54, and `Queued background download for kkzu-unattributed` in 3. It
        // WAS the eighth and last `downloadTask(with:)` this suite issued,
        // ~1.3 s after the other seven, by which time seven live transfers to a
        // non-resolving host had `nsurlsessiond` busy — those seven are queued
        // successfully in 56 of the 57. A private queue does not change that
        // (see `unsharedSessionIO`), so a witness here would have failed 54
        // runs out of 57. Written directly, the record is present by
        // construction, the assertion below is sensitive to the CONTEXT, and
        // the suite stops issuing the one call that stalls every merge gate.
        let unattributed = "kkzu-unattributed"
        await manager.persistDownloadAttribution(
            episodeId: unattributed,
            context: .unattributed(
                reason: .resumeWithoutRecordedShow,
                isExplicitDownload: false
            )
        )
        #expect(
            await manager.loadDownloadAttribution(episodeId: unattributed)
                != nil,
            """
            no unattributed record reached the completion — its \
            podcastId == nil below would be reporting a MISSING sidecar \
            rather than a recorded absence of a show
            """
        )
        let unattributedJob = try await completeBackgroundDownload(
            manager: manager, store: store,
            episodeId: unattributed, cacheDir: dir
        )
        #expect(
            unattributedJob != nil,
            "the contrast arm must enqueue too, or it proves nothing"
        )
        #expect(unattributedJob?.podcastId == nil)
    }

    // MARK: - R2: the attribution survives a process restart

    /// iOS relaunches the app to deliver `handleEventsForBackgroundURLSession`,
    /// so the completion that enqueues analysis routinely runs in a DIFFERENT
    /// PROCESS from the `backgroundDownload` that started the transfer. This
    /// rail models that by handing the completion to a second `DownloadManager`
    /// built over the same cache directory — a fresh actor with no in-memory
    /// state whatsoever. An in-memory `[episodeId: context]` map passes R1 and
    /// fails here, which is exactly why the attribution is on disk.
    @Test("A completion delivered to a NEW manager instance still carries the show")
    func attributionSurvivesProcessRestart() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeTestStore()

        let queueing = await makeManager(cacheDirectory: dir)
        try await queueing.bootstrap()
        let episodeId = "kkzu-restart"
        await queueing.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: DownloadContext(
                podcastId: Self.showId, isExplicitDownload: false
            )
        )

        // A brand-new manager: nothing carries over but the filesystem.
        let relaunched = await makeManager(cacheDirectory: dir)
        await relaunched.setAnalysisWorkScheduler(makeScheduler(store: store))
        try await relaunched.bootstrap()
        #expect(
            await relaunched.loadDownloadAttribution(episodeId: episodeId)?
                .podcastId == Self.showId,
            "the relaunched manager must recover the show from disk"
        )

        let job = try await completeBackgroundDownload(
            manager: relaunched, store: store,
            episodeId: episodeId, cacheDir: dir
        )
        #expect(job != nil, "positive witness: the relaunched completion enqueued")
        #expect(job?.podcastId == Self.showId)

        // Negative witness: the fresh manager genuinely had no in-memory
        // knowledge — an episode it was never told about resolves to nothing.
        #expect(
            await relaunched.loadDownloadAttribution(
                episodeId: "kkzu-never-queued"
            ) == nil
        )
    }

    // MARK: - R3: the record is written, then reaped

    @Test("Queueing writes the attribution and a terminal completion reaps it")
    func attributionIsWrittenThenReaped() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeTestStore()
        let manager = await makeManager(cacheDirectory: dir)
        await manager.setAnalysisWorkScheduler(makeScheduler(store: store))
        try await manager.bootstrap()

        let episodeId = "kkzu-reaped"
        // Positive witness FIRST: absent before, present after — so the
        // "absent afterwards" assertion cannot pass because nothing was
        // ever written.
        #expect(await manager.loadDownloadAttribution(episodeId: episodeId) == nil)
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: DownloadContext(
                podcastId: Self.showId, isExplicitDownload: false
            )
        )
        #expect(
            await manager.loadDownloadAttribution(episodeId: episodeId)?
                .podcastId == Self.showId
        )

        try await completeBackgroundDownload(
            manager: manager, store: store,
            episodeId: episodeId, cacheDir: dir
        )
        #expect(
            await manager.loadDownloadAttribution(episodeId: episodeId) == nil,
            "a terminal completion must reap the record so it cannot accumulate"
        )
    }

    /// A SUSPENDED transfer is not terminal, and the force-quit resume path
    /// (`ForceQuitResumeScan`, which runs after a relaunch with no SwiftData
    /// in scope) recovers the show from this record. So dropping the
    /// resume-data blob must NOT drop the attribution with it.
    @Test("Dropping the resume blob leaves the attribution for the resume path")
    func resumeBlobDeletionPreservesAttribution() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = await makeManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "kkzu-suspended"
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: DownloadContext(
                podcastId: Self.showId, isExplicitDownload: false
            )
        )
        try await manager.persistResumeData(
            episodeId: episodeId,
            data: Data(repeating: 0x11, count: 32)
        )
        // Positive witness: the blob was really there to delete.
        #expect(try await manager.loadResumeData(episodeId: episodeId) != nil)

        try await manager.deleteResumeData(episodeId: episodeId)

        #expect(try await manager.loadResumeData(episodeId: episodeId) == nil)
        #expect(
            await manager.loadDownloadAttribution(episodeId: episodeId)?
                .podcastId == Self.showId,
            "the resume path's only route back to the show must survive a blob drop"
        )
    }

    /// The force-quit resume path in full. When the server has rotated its
    /// stitch, `resumeSuspendedTransfer` discards the blob and re-queues a
    /// FRESH background download — from a `DownloadManager` extension with no
    /// SwiftData in scope, so the sidecar is its only route back to the show.
    ///
    /// The assertion works because the re-queue REWRITES the record from the
    /// context it was handed: a resume that passed an unattributed context
    /// would overwrite the good record with a null one.
    @Test("A force-quit resume re-queues the fresh download against the recovered show")
    func forceQuitResumeRecoversTheShow() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = await makeManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "kkzu-force-quit"
        let src = URL(string: "https://dai.example.com/\(episodeId).mp3")!
        // A PREVIOUS process queued this transfer and recorded its show; this
        // process only inherits the filesystem. Written directly rather than
        // via `backgroundDownload` so the episode does not hold this
        // instance's in-flight slot, which is exactly the state a relaunch
        // starts from.
        await manager.persistDownloadAttribution(
            episodeId: episodeId,
            context: DownloadContext(
                podcastId: Self.showId, isExplicitDownload: false
            )
        )
        try await manager.persistResumeData(
            episodeId: episodeId,
            data: Data([0x01, 0x02, 0x03]),
            sourceURL: src,
            validator: HTTPAssetMetadata(
                etag: "\"A\"", contentLength: 100, lastModified: nil
            )
        )
        // The server now serves a different stitch, so the blob is unusable
        // and the fresh-redownload branch is the one that runs.
        await manager.setResumeValidatorProviderForTesting { _ in
            HTTPAssetMetadata(
                etag: "\"B\"", contentLength: 80, lastModified: nil
            )
        }

        let outcome = try await manager.resumeSuspendedTransfer(
            episodeId: episodeId
        )

        // Positive witness: the branch under test is the one that ran.
        #expect(outcome == .redownloadedFresh)
        #expect(
            await manager.loadDownloadAttribution(episodeId: episodeId)?
                .podcastId == Self.showId,
            "the re-queued fresh download must carry the show recovered from disk"
        )

        await manager.invalidateBackgroundSessionsForTesting()
    }

    /// An explicit cancel IS terminal, so the record goes with it.
    @Test("Cancelling a download reaps its attribution")
    func cancelReapsAttribution() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = await makeManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "kkzu-cancelled"
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: DownloadContext(
                podcastId: Self.showId, isExplicitDownload: false
            )
        )
        #expect(
            await manager.loadDownloadAttribution(episodeId: episodeId) != nil,
            "positive witness: there was a record to reap"
        )

        await manager.cancelDownload(episodeId: episodeId)

        #expect(await manager.loadDownloadAttribution(episodeId: episodeId) == nil)
    }

    /// A cache clear is an ownership boundary for every per-transfer artifact.
    /// Attribution is one, so it goes with the bytes it describes rather than
    /// outliving them.
    @Test("Clearing the cache takes the attribution with it")
    func clearCacheReapsAttribution() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = await makeManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "kkzu-cleared"
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: DownloadContext(
                podcastId: Self.showId, isExplicitDownload: false
            )
        )
        #expect(
            await manager.loadDownloadAttribution(episodeId: episodeId) != nil,
            "positive witness: there was a record for the clear to take"
        )

        try await manager.clearCache()

        #expect(await manager.loadDownloadAttribution(episodeId: episodeId) == nil)
        await manager.invalidateBackgroundSessionsForTesting()
    }

    // MARK: - R4: an absence cannot be anonymous

    /// The type-level half of the fix. A caller can no longer arrive at a null
    /// show by omission — `DownloadContext(podcastId:)` takes a non-optional
    /// `String`, so every nil in the system came from a named constructor.
    @Test("A null show identity always carries the reason it is null")
    func nullIdentityIsAlwaysNamed() {
        let named = DownloadContext.unattributed(
            reason: .resumeWithoutRecordedShow, isExplicitDownload: false
        )
        #expect(named.podcastId == nil)
        #expect(named.unattributedReason == .resumeWithoutRecordedShow)

        let resolvedNil = DownloadContext.resolving(
            podcastId: nil,
            unattributedReason: .showIdentityUnresolvable,
            isExplicitDownload: false
        )
        #expect(resolvedNil.podcastId == nil)
        #expect(resolvedNil.unattributedReason == .showIdentityUnresolvable)

        // Positive witness: the SAME constructor with a real show records no
        // reason, so "reason != nil" above is not simply always set.
        let resolvedSome = DownloadContext.resolving(
            podcastId: Self.showId,
            unattributedReason: .showIdentityUnresolvable,
            isExplicitDownload: false
        )
        #expect(resolvedSome.podcastId == Self.showId)
        #expect(resolvedSome.unattributedReason == nil)
    }

    /// `""` is the sibling defect of NULL: it is not a show, but unlike NULL
    /// it JOINS — every unattributed episode collapses into one fake show.
    /// A blank or non-canonical identifier therefore becomes a named absence
    /// rather than a key.
    @Test("An empty or non-canonical identifier is a named absence, not a key")
    func blankIdentityIsRejected() {
        for blank in ["", "   ", "\n"] {
            let ctx = DownloadContext(
                podcastId: blank, isExplicitDownload: false
            )
            #expect(
                ctx.podcastId == nil,
                "\(blank.debugDescription) must not become a joinable show key"
            )
            #expect(ctx.unattributedReason == .showIdentityUnresolvable)
        }

        // Untrimmed spelling is not this show's canonical identity either —
        // admitting it would retarget show-scoped evidence to a neighbouring
        // namespace.
        let untrimmed = DownloadContext(
            podcastId: " \(Self.showId) ", isExplicitDownload: false
        )
        #expect(untrimmed.podcastId == nil)

        // Positive witness: the canonical spelling IS admitted, so the rail
        // above is not just "this initializer always yields nil".
        let canonical = DownloadContext(
            podcastId: Self.showId, isExplicitDownload: false
        )
        #expect(canonical.podcastId == Self.showId)
        #expect(canonical.unattributedReason == nil)
    }

    // MARK: - R5: the played path is unchanged

    /// The acceptance negative. `PlayheadRuntime`'s streaming branch moved
    /// from `DownloadContext(podcastId:)` to `.resolving(podcastId:)` because
    /// `Episode.resolvedShowIdentity` is legitimately nullable. For every
    /// resolvable show the two must be indistinguishable — that path already
    /// worked and this bead must not perturb it.
    @Test("The played path's context is byte-identical under the new constructor")
    func playedPathContextIsUnchanged() {
        let direct = DownloadContext(
            podcastId: Self.showId,
            isExplicitDownload: false,
            podcastTitle: "Diary of a CEO",
            episodeTitle: "Episode 1"
        )
        let viaResolving = DownloadContext.resolving(
            podcastId: Self.showId,
            unattributedReason: .showIdentityUnresolvable,
            isExplicitDownload: false,
            podcastTitle: "Diary of a CEO",
            episodeTitle: "Episode 1"
        )
        #expect(direct == viaResolving)

        // Positive witness: the comparison can fail. A different show is not
        // equal, so the equality above is a real check and not `nil == nil`.
        #expect(
            direct != DownloadContext.resolving(
                podcastId: "https://feeds.example.com/other.xml",
                unattributedReason: .showIdentityUnresolvable,
                isExplicitDownload: false,
                podcastTitle: "Diary of a CEO",
                episodeTitle: "Episode 1"
            )
        )
    }
}
