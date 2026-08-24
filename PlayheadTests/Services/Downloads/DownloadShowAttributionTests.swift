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
// playhead-et2d: EVERY `DownloadManager` HERE IS BUILT BY `makeManager`, which
// injects `unsharedSessionIO()` and proves the queue it got is private. The
// default `BackgroundSessionIO.shared` is a process-wide singleton with ONE
// serial queue, and over 57 de-duplicated full-plan logs that coupling cost
// this suite in both directions — blocked by `ForceQuitResumeTests` in 7 of
// the 13 logs before playhead-7wia, and on 2026-08-23 parking the queue with
// its own `kkzu-cleared` while twelve transfers drained late across three
// suites and eleven tests failed. Seven of those twelve and seven of those
// eleven are ITS OWN; the traffic it cost other people is five transfers and
// four tests. ``unsharedSessionIO`` carries the whole
// measurement, what a private queue does NOT buy, and why a green run in this
// file is weak evidence that it worked.
//
// WHY THOSE FAILURES ARE RECORDED AS ASSERTIONS, which is what made the cause
// unreadable from the baseline file: all three no-answer branches of
// `backgroundDownload` DELETE the attribution sidecar, so this suite reads a
// missing show and blames the code that writes it. Same reason `.neverAnswers`
// (playhead-7wia's fix for the other three victims) cannot be reused here —
// these tests need a genuinely ADMITTED transfer.

import Foundation
import Testing
@testable import Playhead

@Suite("Download show attribution (playhead-kkzu)")
struct DownloadShowAttributionTests {

    private static let showId = "https://feeds.example.com/diary.xml"

    /// Builds this suite's `DownloadManager` and PROVES, before handing it
    /// back, that it is not on the process-wide `BackgroundSessionIO` queue.
    ///
    /// playhead-et2d. The injection is the whole fix; without a rail, the only
    /// thing between "every manager here has its own queue" and "one quietly
    /// went back to `.shared`" is eight identical argument lists nobody
    /// re-reads. Getting one wrong fails only under a full plan's concurrency,
    /// and only when some suite parks the daemon — so it reads as this file's
    /// defect and is not.
    ///
    /// TWO CHECKS, because the first cannot see the likeliest regression. It
    /// compares ONE label against ONE constant, so a helper that MEMOIZES —
    /// hands the same `BackgroundSessionIO`, hence the same serial queue, to
    /// all eight managers — keeps a non-default label and stays green while the
    /// suite is back in exactly the state this bead removed. "Memoize the
    /// factory" is the ordinary shape of a future tidy-up, so the second check
    /// asks the helper for another instance and requires a different label. It
    /// costs one throwaway `BackgroundSessionIO` per manager, whose `init`
    /// builds two dispatch queues, touches no daemon, and is released with the
    /// expression.
    ///
    /// The label is a PROXY for queue identity, sound only in the safe
    /// direction: `init` builds the queue FROM the label, so different labels
    /// mean different queues, while two instances sharing a label still own two
    /// queues. This check can cry wolf; it cannot miss a shared queue.
    ///
    /// A THIRD CHECK PINS THE BOUND, because the two headers assert it and
    /// nothing enforced it: widening `unsharedSessionIO`'s `timeout` would
    /// trade this suite's assertion failure for a time-limit failure — 7wia
    /// measured that at 60 s these calls are still QUEUED — and no rail in the
    /// tree would have reported it.
    ///
    /// A FOURTH CHECK PINS THE BEHAVIOUR, and it exists because this comment
    /// used to say it could not. The withdrawn sentence read: "`behavior` is
    /// NOT pinned and cannot be: `BackgroundSessionIO.Behavior` is not
    /// `Equatable`, and the one observation that separates `.dedicatedThread`
    /// from a refusing seam is a `perform`, which would make a real daemon
    /// call from a helper." NOT `Equatable` rules out `==`; it does not make
    /// an enum unobservable. `behavior` is a stored `let`
    /// (`BackgroundSessionIO.swift:141`) and `if case .dedicatedThread =`
    /// separates the production case from all three seams with no `perform`,
    /// no queue submission and no daemon call. A rail left out on the strength
    /// of an impossibility that is not one is the same shape as the bound the
    /// paragraph above describes: asserted by two headers, enforced by nothing.
    ///
    /// WHAT IT CATCHES THAT NOTHING ELSE CAN. Mutant E3 (`.neverAnswers`) was
    /// the evidence offered for leaving it out, and E3 cannot serve: it kills
    /// through the ASSERTIONS, because a refused transfer deletes the sidecar,
    /// so it reports the same seven victims with or without a rail. The mutant
    /// that separates them is E12 —
    /// `.refusesCallsLabelled("<a marker no call label contains>")`. `perform`
    /// matches that marker with `label.contains(marker)`
    /// (`BackgroundSessionIO.swift:240`), so it refuses NOTHING and is
    /// behaviourally identical to `.dedicatedThread` on every call this suite
    /// makes. It is invisible to the three checks above and to every
    /// assertion in the file, and it SURVIVED the battery until this check
    /// existed.
    ///
    /// STILL OPEN: (a) nothing enforces the `makeManager` route — mutant E10
    /// measures that by bypassing it; (b) two calls through ONE manager's
    /// `sessionIO` share one queue, which no mutant can measure because it is a
    /// property of `DownloadManager.init` rather than a regression. Note (b)
    /// is about `sessionIO` and not about the manager: `init` builds THREE
    /// `BackgroundSessionIO`s, adding `enumerationIO` and `sessionCreationIO`
    /// through `onItsOwnQueue`. Both take their labels FROM
    /// `sessionIO.queueLabel`, so one injection privatises all three.
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
            (\(io.queueLabel)) — so its downloadTask(with:) can be refused by \
            any parked call in the plan AND can itself park that queue for \
            every other suite on it, which is the coupling playhead-et2d \
            removed. Both directions are measured over 57 full-plan logs: \
            this suite was blocked by ForceQuitResumeTests in 7 of them, and \
            on 2026-08-23 its own kkzu-cleared held the queue 65 s and eleven \
            tests in three files failed
            """,
            sourceLocation: sourceLocation
        )
        #expect(
            unsharedSessionIO(labelledFor: test).queueLabel != io.queueLabel,
            """
            unsharedSessionIO returned two instances with the SAME queue \
            label (\(io.queueLabel)). Either it now MEMOIZES — one serial \
            queue for all eight managers, the state playhead-et2d removed, \
            invisible to the check above because the label is not the \
            default — or its label stopped being unique per call, which gives \
            two DISTINCT queues one name and is a false alarm this rail \
            cannot tell apart. Only the first is the regression
            """,
            sourceLocation: sourceLocation
        )
        #expect(
            io.timeout == BackgroundSessionIO.defaultTimeout,
            """
            unsharedSessionIO is no longer on the production bound \
            (\(io.timeout)s vs \(BackgroundSessionIO.defaultTimeout)s). A \
            widened bound is legitimate in a double that needs the daemon to \
            answer under load (BackgroundDownloadDropLedgerTests.answeringIO), \
            and it is wrong here: playhead-7wia measured that at 60 s these \
            calls are still QUEUED, so widening trades this suite's assertion \
            failure for a time-limit failure and hides the queue entirely
            """,
            sourceLocation: sourceLocation
        )
        let behaviourIsProduction: Bool
        if case .dedicatedThread = io.behavior {
            behaviourIsProduction = true
        } else {
            behaviourIsProduction = false
        }
        #expect(
            behaviourIsProduction,
            """
            unsharedSessionIO is no longer on the production BEHAVIOUR. Every \
            other case of BackgroundSessionIO.Behavior is a #if DEBUG test \
            seam that refuses calls, and a seam whose marker matches no call \
            label refuses nothing at all — so it reads exactly like \
            .dedicatedThread here while making this helper a double of \
            something other than production. The queue, the bound and the \
            label checks above cannot see it, and neither can any assertion \
            in this file
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
        // `backgroundDownload` — the idiom `forceQuitResumeRecoversTheShow`
        // below already uses. WHAT THAT GIVES UP, stated rather than left to
        // be discovered: this was the only place in the tree that drove an
        // `.unattributed` context THROUGH `backgroundDownload` to the sidecar
        // and back, so nothing now covers `backgroundDownload`'s own write of
        // an unattributed record. It was never asserted coverage — mutant E8P
        // proves it, surviving against the pre-bead file on a run that DID
        // admit the transfer — but it was reachable, and it is not any more. This arm ASSERTS AN ABSENCE, and a refused
        // transfer produces the same absence: all three no-answer branches
        // delete the sidecar, and a completion that finds none defaults to
        // `.unattributed(.resumeWithoutRecordedShow)`, byte-identical to the
        // context passed here. So `podcastId == nil` below used to read the
        // same whether the download was admitted or never started — this
        // repo's standing defect class, sitting inside the assertion.
        //
        // MEASURED, and it is why the arm moved off the daemon instead of
        // getting a witness: over 57 de-duplicated full-plan logs, 2026-08-15
        // … 08-24, `kkzu-unattributed`'s `downloadTask(with:)` EXPIRED in 54
        // and was queued in 3, so a witness on the OLD arm would have failed
        // 54 runs of 57. "Expired", not "refused": `.dedicatedThread` submits
        // the call and reports `did not answer within 10s`; "refused" is what
        // the `.neverAnswers` / `.refusesCallsLabelled` seams do, and the two
        // are different events. It was the eighth and last `downloadTask(with:)`
        // this suite issued, a median 1.573 s after the other seven (mean
        // 1.601, range 0.404–3.309, over the 51 logs carrying both anchors),
        // by which time seven live transfers to a non-resolving host had
        // `nsurlsessiond` busy — and those seven queue successfully in 56 of
        // the 57. EVERY ONE OF THOSE 57 IS PRE-FIX, measured while this suite
        // was on the SHARED queue; what the rate would be on a private queue
        // is unmeasured, and ``unsharedSessionIO`` says why that is not
        // knowable from these logs.
        //
        // THE WRITTEN CONTEXT IS DELIBERATELY NOT THE FALLBACK (review r5).
        // Writing `isExplicitDownload: false` would have reproduced the
        // defect one level up: the sidecar would then be byte-identical to
        // `handleBackgroundDownloadComplete`'s own default, so `podcastId ==
        // nil` below still could not tell "the completion READ the record"
        // from "the completion ignored it and fell back". `true` makes the
        // record observable — `AnalysisWorkScheduler.enqueue` computes
        // `priority = userInitiated ? 20 : (isExplicitDownload ? 10 : 0)`, and
        // no user intent is marked here — so the `priority == 10` expectation
        // below fails if the fallback is what reached the scheduler. Without
        // it this arm asserts only that a sidecar EXISTED.
        let unattributed = "kkzu-unattributed"
        await manager.persistDownloadAttribution(
            episodeId: unattributed,
            context: .unattributed(
                reason: .resumeWithoutRecordedShow,
                isExplicitDownload: true
            )
        )
        #expect(
            await manager.loadDownloadAttribution(episodeId: unattributed)
                != nil,
            """
            the unattributed record was not written, so podcastId == nil \
            below would be reporting a MISSING sidecar rather than a \
            recorded absence of a show
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
        #expect(
            unattributedJob?.priority == 10,
            """
            the enqueued job did not carry the isExplicitDownload:true this \
            arm wrote to the sidecar (priority \
            \(unattributedJob?.priority.description ?? "nil") vs 10), so the \
            completion used its own .unattributed(.resumeWithoutRecordedShow, \
            isExplicitDownload: false) fallback instead of reading the \
            record. podcastId == nil above cannot see that, because the \
            fallback's podcastId is nil too
            """
        )
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
